"""
Weather.com Provider for Duck Sun Modesto

Scrapes 10-day forecast from Weather.com for Downtown Modesto, CA.
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
        Synchronously fetch 10-day forecast from Weather.com's API.

        Returns:
            List of WeatherComDay dicts, or None on failure
        """
        if not HAS_CURL_CFFI:
            logger.error("[WeatherComProvider] Missing curl_cffi dependency")
            return None

        # Construct API URL with parameters
        # Weather.com uses this API key for public access
        api_key = "e1f10a1e78da46f5b10a1e78da96f525"  # Public TWC API key
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
                response = session.get(url, headers=headers, timeout=30)

            logger.info(f"[WeatherComProvider] API Response status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"[WeatherComProvider] API HTTP {response.status_code} - likely IP blocked or API key invalid")
                # Fall back to scraping if API fails
                return self._fetch_via_scraping()

            data = response.json()

            # Extract daily forecast data
            day_of_week = data.get('dayOfWeek', [])
            temp_max = data.get('temperatureMax', [])
            temp_min = data.get('temperatureMin', [])
            narrative = data.get('narrative', [])

            if not temp_max or not temp_min:
                logger.error("[WeatherComProvider] No temperature data in API response")
                return self._fetch_via_scraping()

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

                # Get condition from narrative
                condition = narrative[i][:50] if i < len(narrative) else "Unknown"

                results.append({
                    "date": date_str,
                    "day_name": day_name,
                    "high_f": float(high_f),
                    "low_f": float(low_f),
                    "high_c": round(high_c, 2),
                    "low_c": round(low_c, 2),
                    "condition": condition,
                    "precip_prob": 0
                })

                logger.debug(f"[WeatherComProvider] {date_str}: Hi={high_f}F, Lo={low_f}F")

            logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} daily records from API")
            return results

        except Exception as e:
            logger.error(f"[WeatherComProvider] API fetch failed: {e}", exc_info=True)
            return self._fetch_via_scraping()

    def _fetch_via_scraping(self) -> Optional[List[WeatherComDay]]:
        """Fallback to web scraping if API fails."""
        logger.info("[WeatherComProvider] Falling back to web scraping...")

        scrape_url = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

        try:
            from curl_cffi.requests import Session

            # Use a session to handle cookies - first visit homepage to get session cookies
            with Session(impersonate="chrome110") as session:
                # First request to get cookies
                logger.debug("[WeatherComProvider] Getting session cookies from homepage...")
                home_resp = session.get("https://weather.com/", timeout=15)
                logger.debug(f"[WeatherComProvider] Homepage status: {home_resp.status_code}")

                # Now fetch the forecast page with cookies
                response = session.get(scrape_url, timeout=30)

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
                    "precip_prob": 0
                })

            logger.info(f"[WeatherComProvider] [OK] Retrieved {len(results)} records via scraping")
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
        print("-" * 50)
        print(f"{'Day':<15} | {'High':<6} | {'Low':<6}")
        print("-" * 35)
        for day in data:
            print(f"{day['day_name']:<15} | {day['high_f']:.0f}F    | {day['low_f']:.0f}F")
    else:
        print("[FAILED] Could not fetch Weather.com data")

    print("\n" + "=" * 60)
