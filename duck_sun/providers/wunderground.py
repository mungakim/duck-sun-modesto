"""
Weather Underground Provider for Duck Sun Modesto

Scrapes 10-day forecast from Weather Underground for Modesto, CA (95350).
Uses curl_cffi with Chrome impersonation to bypass anti-bot measures.

Weight: 4.0 (same as AccuWeather - commercial provider)
"""

import logging
import re
from datetime import datetime, timedelta
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
    Provider for Weather Underground data via web scraping.

    Uses curl_cffi with Chrome impersonation to bypass anti-bot protection.
    Extracts forecast data from embedded JSON in the page's script tags.

    Weight: 4.0 (commercial provider tier)
    """

    # Modesto, CA ZIP code URL
    URL = "https://www.wunderground.com/forecast/us/ca/modesto/95350"

    def __init__(self):
        logger.info("[WUndergroundProvider] Initializing provider...")
        if not HAS_CURL_CFFI:
            logger.warning("[WUndergroundProvider] curl_cffi not installed - provider disabled")
        if not HAS_BS4:
            logger.warning("[WUndergroundProvider] beautifulsoup4 not installed - provider disabled")

    def _extract_array(self, pattern: str, data: str, is_numeric: bool = True) -> List:
        """Extract array values from JavaScript data using regex."""
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
                            values.append(None)
                return values
            else:
                return [x.strip().strip('"') for x in arr_str.split(',')]
        return []

    def _get_date_for_day(self, day_index: int) -> str:
        """Get YYYY-MM-DD date string for day index (0 = today)."""
        tz = ZoneInfo("America/Los_Angeles")
        today = datetime.now(tz)
        target = today + timedelta(days=day_index)
        return target.strftime('%Y-%m-%d')

    def fetch_sync(self) -> Optional[List[WUndergroundDay]]:
        """
        Synchronously fetch 10-day forecast from Weather Underground.

        Parses the embedded JSON data in the page's script tags.

        Returns:
            List of WUndergroundDay dicts, or None on failure
        """
        if not HAS_CURL_CFFI or not HAS_BS4:
            logger.error("[WUndergroundProvider] Missing dependencies (curl_cffi, beautifulsoup4)")
            return None

        logger.info(f"[WUndergroundProvider] Fetching from {self.URL}")

        try:
            # Use Firefox impersonation with Session
            from curl_cffi.requests import Session

            with Session(impersonate="firefox135") as session:
                response = session.get(self.URL, timeout=30)

            if response.status_code != 200:
                logger.error(f"[WUndergroundProvider] HTTP {response.status_code}")
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the script tag containing forecast data
            scripts = soup.find_all('script')
            forecast_data = None

            for script in scripts:
                if script.string and 'dayOfWeek' in script.string and 'temperatureMax' in script.string:
                    forecast_data = script.string
                    break

            if not forecast_data:
                logger.error("[WUndergroundProvider] No forecast data found in page scripts")
                return None

            # Extract arrays from the JavaScript data
            days_of_week = self._extract_array(
                r'"dayOfWeek":\[([^\]]+)\]',
                forecast_data,
                is_numeric=False
            )
            # Use temperatureMax/Min (not calendarDay versions - those are offset)
            max_temps = self._extract_array(
                r'"temperatureMax":\[([^\]]+)\]',
                forecast_data
            )
            min_temps = self._extract_array(
                r'"temperatureMin":\[([^\]]+)\]',
                forecast_data
            )

            if not days_of_week or not max_temps or not min_temps:
                logger.error("[WUndergroundProvider] Could not parse forecast arrays")
                logger.debug(f"[WUndergroundProvider] days_of_week: {len(days_of_week)}")
                logger.debug(f"[WUndergroundProvider] max_temps: {len(max_temps)}")
                logger.debug(f"[WUndergroundProvider] min_temps: {len(min_temps)}")
                return None

            results: List[WUndergroundDay] = []
            num_days = min(10, len(days_of_week), len(max_temps), len(min_temps))

            logger.info(f"[WUndergroundProvider] Found {num_days} forecast days")

            for i in range(num_days):
                high_f = max_temps[i]
                low_f = min_temps[i]

                if high_f is None or low_f is None:
                    logger.warning(f"[WUndergroundProvider] Null temps for day {i}, skipping")
                    continue

                # Convert to Celsius
                high_c = (high_f - 32) * 5 / 9
                low_c = (low_f - 32) * 5 / 9

                # Get day name (truncate to 3 chars)
                day_abbrev = days_of_week[i][:3] if days_of_week[i] else f"D{i}"

                # Get date
                date_str = self._get_date_for_day(i)

                results.append({
                    "date": date_str,
                    "day_name": day_abbrev,
                    "high_f": float(high_f),
                    "low_f": float(low_f),
                    "high_c": round(high_c, 2),
                    "low_c": round(low_c, 2),
                    "condition": "Unknown",  # Could parse from narratives if needed
                    "precip_prob": 0  # Could parse from precipChance array
                })

                logger.debug(f"[WUndergroundProvider] {date_str}: Hi={high_f}F, Lo={low_f}F")

            logger.info(f"[WUndergroundProvider] [OK] Retrieved {len(results)} daily records")
            return results

        except Exception as e:
            logger.error(f"[WUndergroundProvider] Fetch failed: {e}", exc_info=True)
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
        print(f"{'Day':<15} | {'High':<6} | {'Low':<6}")
        print("-" * 35)
        for day in data:
            print(f"{day['day_name']:<15} | {day['high_f']:.0f}F    | {day['low_f']:.0f}F")
    else:
        print("[FAILED] Could not fetch Weather Underground data")

    print("\n" + "=" * 60)
