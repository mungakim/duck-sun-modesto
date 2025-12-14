"""
Weather.com Provider for Duck Sun Modesto

NOTE: Weather.com is JavaScript-rendered and cannot be scraped without
browser automation (Playwright/Selenium). The previous wttr.in proxy
approach was found to provide INACCURATE data.

Current approach: Manual data entry until Playwright can be configured.
The user provides Weather.com 10-day forecast data which is cached.

TODO: Implement Playwright scraping when system dependencies are available.
"""

import httpx
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, TypedDict

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "weathercom_cache.json"
CACHE_TTL_HOURS = 6  # Longer TTL since manual updates


class WeatherComDay(TypedDict):
    """Daily forecast data from Weather.com."""
    date: str
    high_f: Optional[int]
    low_f: int
    high_c: Optional[float]
    low_c: float
    condition: str
    precip_prob: int


class WeatherComProvider:
    """
    Weather.com provider for Modesto, CA forecasts.

    Due to Weather.com being JavaScript-rendered, this provider:
    1. Uses manually-entered data from the actual Weather.com website
    2. Caches data for 6 hours
    3. Falls back to cached data if available

    Weight in ensemble: 2.0 (user baseline reference)
    """

    # Weather.com URL for reference (JS-rendered, not directly scrapable)
    WEATHER_COM_URL = "https://weather.com/weather/tenday/l/USCA0714"

    def __init__(self):
        logger.info("[WeatherComProvider] Initializing provider...")
        CACHE_DIR.mkdir(exist_ok=True)

    def _load_cache(self) -> Optional[dict]:
        """Load cached data if within TTL."""
        if not CACHE_FILE.exists():
            return None

        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60

            logger.info(f"[WeatherComProvider] Cache age: {age_minutes:.1f} minutes")

            if age <= timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"[WeatherComProvider] Cache VALID (TTL: {CACHE_TTL_HOURS}h)")
                return cache
            else:
                logger.info("[WeatherComProvider] Cache EXPIRED")
                return None

        except Exception as e:
            logger.warning(f"[WeatherComProvider] Cache load error: {e}")
            return None

    def _save_cache(self, data: List[WeatherComDay]) -> bool:
        """Save forecast data to cache."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'source': 'weather.com (manual entry)',
                'ttl_hours': CACHE_TTL_HOURS,
                'data': data
            }

            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)

            logger.info(f"[WeatherComProvider] Cache saved: {len(data)} days")
            return True

        except Exception as e:
            logger.error(f"[WeatherComProvider] Cache save failed: {e}")
            return False

    def _get_current_forecast(self) -> List[WeatherComDay]:
        """
        Return current Weather.com forecast data.

        This data was manually extracted from:
        https://weather.com/weather/tenday/l/USCA0714
        As of: December 13, 2025 5:51 PM PST

        TODO: Replace with Playwright scraping when available.
        """
        # Base date for calculating forecast dates
        base_date = datetime(2025, 12, 13)

        # Weather.com 10-day forecast (manually entered)
        forecast_data = [
            # Tonight/Today (Dec 13) - Tonight shows --/38
            {"day_offset": 0, "high_f": None, "low_f": 38, "condition": "Foggy", "precip": 17},
            # Sun 14 - AM Fog/PM Sun
            {"day_offset": 1, "high_f": 49, "low_f": 40, "condition": "AM Fog/PM Sun", "precip": 12},
            # Mon 15 - AM Fog/PM Sun
            {"day_offset": 2, "high_f": 53, "low_f": 44, "condition": "AM Fog/PM Sun", "precip": 8},
            # Tue 16 - Partly Cloudy
            {"day_offset": 3, "high_f": 55, "low_f": 50, "condition": "Partly Cloudy", "precip": 13},
            # Wed 17 - AM Clouds/PM Sun
            {"day_offset": 4, "high_f": 61, "low_f": 48, "condition": "AM Clouds/PM Sun", "precip": 13},
            # Thu 18 - Partly Cloudy
            {"day_offset": 5, "high_f": 58, "low_f": 48, "condition": "Partly Cloudy", "precip": 8},
            # Fri 19 - Showers
            {"day_offset": 6, "high_f": 56, "low_f": 51, "condition": "Showers", "precip": 44},
            # Sat 20 - Showers
            {"day_offset": 7, "high_f": 53, "low_f": 49, "condition": "Showers", "precip": 50},
            # Sun 21 - Showers
            {"day_offset": 8, "high_f": 54, "low_f": 50, "condition": "Showers", "precip": 50},
        ]

        results: List[WeatherComDay] = []

        for entry in forecast_data:
            forecast_date = base_date + timedelta(days=entry["day_offset"])
            date_str = forecast_date.strftime("%Y-%m-%d")

            # Convert F to C
            high_f = entry["high_f"]
            low_f = entry["low_f"]
            high_c = round((high_f - 32) * 5 / 9, 1) if high_f else None
            low_c = round((low_f - 32) * 5 / 9, 1)

            results.append({
                "date": date_str,
                "high_f": high_f,
                "low_f": low_f,
                "high_c": high_c,
                "low_c": low_c,
                "condition": entry["condition"],
                "precip_prob": entry["precip"]
            })

        return results

    async def fetch_forecast(self, force_refresh: bool = False) -> Optional[List[WeatherComDay]]:
        """
        Fetch Weather.com forecast data.

        Currently returns manually-entered data from the actual Weather.com site.

        Args:
            force_refresh: If True, regenerate from manual data

        Returns:
            List of WeatherComDay dicts, or None on failure
        """
        # Check cache first
        if not force_refresh:
            cache = self._load_cache()
            if cache and cache.get('data'):
                logger.info("[WeatherComProvider] CACHE HIT - Returning cached data")
                return cache['data']

        logger.info("[WeatherComProvider] Generating Weather.com forecast data...")

        # Get current forecast data (manually entered)
        results = self._get_current_forecast()

        if results:
            logger.info(f"[WeatherComProvider] Retrieved {len(results)} days from Weather.com data")
            self._save_cache(results)
            return results

        return None

    def update_forecast(self, forecast_data: List[dict]) -> bool:
        """
        Update the cached forecast with new manual data.

        Args:
            forecast_data: List of dicts with keys:
                - date: YYYY-MM-DD
                - high_f: High temp in F (or None)
                - low_f: Low temp in F
                - condition: Weather description
                - precip_prob: Precipitation probability %

        Returns:
            True if update successful
        """
        try:
            results: List[WeatherComDay] = []

            for entry in forecast_data:
                high_f = entry.get("high_f")
                low_f = entry["low_f"]

                results.append({
                    "date": entry["date"],
                    "high_f": high_f,
                    "low_f": low_f,
                    "high_c": round((high_f - 32) * 5 / 9, 1) if high_f else None,
                    "low_c": round((low_f - 32) * 5 / 9, 1),
                    "condition": entry.get("condition", "Unknown"),
                    "precip_prob": entry.get("precip_prob", 0)
                })

            return self._save_cache(results)

        except Exception as e:
            logger.error(f"[WeatherComProvider] Update failed: {e}")
            return False

    def get_status(self) -> dict:
        """Get provider status information."""
        cache = self._load_cache()
        return {
            "provider": "Weather.com",
            "status": "manual_entry",
            "note": "JS-rendered site - using manually entered data",
            "cache_valid": cache is not None,
            "url": self.WEATHER_COM_URL
        }


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.DEBUG)

    async def test():
        print("=" * 60)
        print("  WEATHER.COM PROVIDER TEST")
        print("=" * 60)

        provider = WeatherComProvider()

        print("\n[PROVIDER STATUS]")
        status = provider.get_status()
        for key, value in status.items():
            print(f"  {key}: {value}")

        print("\n[FETCHING FORECAST]")
        data = await provider.fetch_forecast(force_refresh=True)

        if data:
            print(f"\n[RESULTS] Weather.com Forecast ({len(data)} days):")
            print("-" * 50)
            for day in data:
                hi = day['high_f'] if day['high_f'] else "--"
                print(f"  {day['date']}: Hi={hi}F, Lo={day['low_f']}F, "
                      f"Precip={day['precip_prob']}%, {day['condition']}")
        else:
            print("[FAILED] Could not fetch Weather.com data")

        print("\n" + "=" * 60)

    asyncio.run(test())
