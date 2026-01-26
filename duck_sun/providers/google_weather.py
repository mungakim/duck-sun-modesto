"""
Google Maps Platform Weather API Provider for Duck Sun Modesto

Powered by Google's "WeatherNext" and "MetNet-3" neural models.
Focus: Hyperlocal precision for Today + Next 3 Days (96 hours).

API DOCS: https://developers.google.com/maps/documentation/weather

WEIGHTING STRATEGY:
- Days 0-3: Weight 6.0 (Primary Source - MetNet-3 Neural Model)
- Superior short-term precision via real-time radar/satellite fusion
- Best for "nowcasting" rather than physics simulations

RATE LIMITING:
- Check Google Cloud Console for quota limits
- Implements pagination for full 96-hour forecasts
"""

import httpx
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, TypedDict, Dict, Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("outputs/cache")
CACHE_FILE = CACHE_DIR / "google_weather_lkg.json"

# SSL verification toggle for corporate proxy environments
SKIP_SSL_VERIFY = os.getenv("DUCK_SUN_SKIP_SSL_VERIFY", "").lower() in ("1", "true", "yes")


class GoogleHourlyData(TypedDict):
    """Hourly forecast data from Google Weather API."""
    time: str
    temp_c: float
    feels_like_c: float
    precip_prob: int
    precip_mm: float
    dewpoint_c: float
    cloud_cover: int
    wind_speed_kmh: float
    condition: str
    is_daytime: bool


class GoogleDailyData(TypedDict):
    """Daily aggregated data for ensemble consensus."""
    date: str
    high_c: float
    low_c: float
    high_f: float
    low_f: float
    precip_prob: int
    condition: str


class GoogleWeatherProvider:
    """
    Provider for Google Maps Weather API.

    Powered by Google's MetNet-3 neural weather model which uses
    satellite imagery and radar fusion for hyperlocal predictions.

    WEIGHT: 6.0 (Highest - Neural/Satellite Fusion)
    - Best accuracy for 0-96 hour forecasts
    - Real-time data fusion vs physics-only models
    """

    # Google Weather API endpoint (Forecast Hours)
    BASE_URL = "https://weather.googleapis.com/v1/forecast/hours:lookup"

    # Modesto, CA coordinates
    LAT = 37.6391
    LON = -120.9969

    # Timezone for Modesto
    TIMEZONE = "America/Los_Angeles"

    def __init__(self):
        logger.info("[GoogleWeatherProvider] Initializing provider...")
        self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            logger.warning("[GoogleWeatherProvider] No API Key found in env!")
        else:
            logger.info("[GoogleWeatherProvider] API key loaded successfully")

        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        logger.debug(f"[GoogleWeatherProvider] Cache directory: {CACHE_DIR.absolute()}")

    def _load_cache(self) -> Optional[Dict]:
        """Load cached data if it exists."""
        if not CACHE_FILE.exists():
            logger.info("[GoogleWeatherProvider] No cache file found")
            return None

        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            cached_time_str = cache.get('timestamp')
            if not cached_time_str:
                logger.warning("[GoogleWeatherProvider] Cache missing timestamp")
                return None

            cached_time = datetime.fromisoformat(cached_time_str)
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60

            logger.info(f"[GoogleWeatherProvider] Cache age: {age_minutes:.1f} minutes")
            return cache

        except Exception as e:
            logger.warning(f"[GoogleWeatherProvider] Cache load error: {e}")
            return None

    def _save_cache(self, hourly_data: List[GoogleHourlyData], daily_data: List[GoogleDailyData]) -> bool:
        """Save forecast data to cache."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'location': f"{self.LAT},{self.LON}",
                'hourly': hourly_data,
                'daily': daily_data
            }

            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)

            logger.info(f"[GoogleWeatherProvider] Cache saved: {len(hourly_data)} hourly, {len(daily_data)} daily records")
            return True

        except Exception as e:
            logger.error(f"[GoogleWeatherProvider] Cache save failed: {e}")
            return False

    def _merge_with_historical(self, new_hourly: List[GoogleHourlyData]) -> List[GoogleHourlyData]:
        """
        Merge new hourly data with cached historical data for today.

        This preserves earlier hours of today that are no longer in the API response
        (e.g., morning duck curve hours when fetching in the evening).
        """
        tz = ZoneInfo(self.TIMEZONE)
        today = datetime.now(tz).strftime('%Y-%m-%d')

        # Load existing cache
        if not CACHE_FILE.exists():
            return new_hourly

        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                old_cache = json.load(f)
            # Handle both direct format and LKG wrapper format
            if 'data' in old_cache and 'hourly' in old_cache['data']:
                existing_hourly = old_cache['data'].get('hourly', [])
            else:
                existing_hourly = old_cache.get('hourly', [])
        except Exception as e:
            logger.debug(f"[GoogleWeatherProvider] Could not load cache for merge: {e}")
            return new_hourly

        if not existing_hourly:
            return new_hourly

        # Build set of times we have new data for
        new_times = {h['time'] for h in new_hourly}

        # Keep old hourly records for today that aren't in new data
        preserved = []
        for old_hour in existing_hourly:
            try:
                time_str = old_hour.get('time', '')
                if 'Z' in time_str:
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00')).astimezone(tz)
                else:
                    dt = datetime.fromisoformat(time_str).astimezone(tz)

                hour_date = dt.strftime('%Y-%m-%d')

                # Keep if it's today and not already in new data
                if hour_date == today and old_hour['time'] not in new_times:
                    preserved.append(old_hour)
                    logger.debug(f"[GoogleWeatherProvider] Preserving historical hour: {time_str}")
            except Exception as e:
                logger.debug(f"[GoogleWeatherProvider] Error checking old hour: {e}")
                continue

        if preserved:
            logger.info(f"[GoogleWeatherProvider] Preserved {len(preserved)} historical hours for today")

        # Merge: preserved old hours + new hours, sorted by time
        merged = preserved + list(new_hourly)
        merged.sort(key=lambda x: x.get('time', ''))

        return merged

    def _get_stale_cache_fallback(self) -> Optional[Dict]:
        """Return stale cache data as fallback when API fails."""
        if CACHE_FILE.exists():
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                if cache.get('hourly') or cache.get('daily'):
                    age_str = cache.get('timestamp', 'unknown')
                    logger.warning(f"[GoogleWeatherProvider] Returning STALE cache as fallback (cached at: {age_str})")
                    return cache
            except Exception as e:
                logger.error(f"[GoogleWeatherProvider] Stale cache fallback failed: {e}")
        return None

    async def fetch_forecast(self, hours: int = 96) -> Optional[Dict[str, Any]]:
        """
        Fetch hourly forecast from Google Weather API.

        Args:
            hours: Number of hours to fetch (default 96 = 4 days)

        Returns:
            Dict with 'hourly' and 'daily' keys containing forecast data,
            or None on failure
        """
        if not self.api_key:
            logger.warning("[GoogleWeatherProvider] Cannot fetch - no API key")
            cache = self._get_stale_cache_fallback()
            return cache

        logger.info(f"[GoogleWeatherProvider] Fetching {hours} hours from Google Weather API...")

        params = {
            "key": self.api_key,
            "location.latitude": self.LAT,
            "location.longitude": self.LON,
            "hours": hours,  # Request full duration - API paginates at 24hrs/page
            "languageCode": "en-US",
            "unitsSystem": "METRIC",
        }

        try:
            async with httpx.AsyncClient(timeout=30.0, verify=not SKIP_SSL_VERIFY) as client:
                all_forecasts = []
                next_page_token = None
                page_count = 0
                max_pages = (hours // 24) + 2  # Calculate needed pages with buffer

                # Fetch loop for pagination
                while len(all_forecasts) < hours and page_count < max_pages:
                    if next_page_token:
                        params["pageToken"] = next_page_token
                    elif "pageToken" in params:
                        del params["pageToken"]

                    logger.debug(f"[GoogleWeatherProvider] Fetching page {page_count + 1}...")
                    resp = await client.get(self.BASE_URL, params=params)

                    if resp.status_code == 401:
                        logger.error("[GoogleWeatherProvider] Unauthorized - check API key")
                        return self._get_stale_cache_fallback()

                    if resp.status_code == 403:
                        logger.error("[GoogleWeatherProvider] Forbidden - API not enabled or quota exceeded")
                        return self._get_stale_cache_fallback()

                    if resp.status_code != 200:
                        logger.error(f"[GoogleWeatherProvider] API Error {resp.status_code} (response body redacted)")
                        return self._get_stale_cache_fallback()

                    data = resp.json()
                    page_forecasts = data.get("forecastHours", [])
                    all_forecasts.extend(page_forecasts)
                    page_count += 1

                    next_page_token = data.get("nextPageToken")
                    if not next_page_token:
                        break

                logger.info(f"[GoogleWeatherProvider] Received {len(all_forecasts)} hourly records ({page_count} pages)")

                # Parse hourly data
                hourly_results = self._parse_hourly_data(all_forecasts)

                # Merge with cached historical data for today (preserves morning hours when fetching in evening)
                hourly_results = self._merge_with_historical(hourly_results)

                # Aggregate to daily for consensus
                daily_results = self._aggregate_to_daily(hourly_results)

                # Cache the results (now includes historical hours)
                self._save_cache(hourly_results, daily_results)

                return {
                    "hourly": hourly_results,
                    "daily": daily_results,
                    "source": "Google Weather API (MetNet-3)",
                    "fetched_at": datetime.now().isoformat()
                }

        except httpx.TimeoutException:
            logger.error("[GoogleWeatherProvider] Request timed out")
            return self._get_stale_cache_fallback()
        except httpx.RequestError as e:
            logger.error(f"[GoogleWeatherProvider] Request error: {e}")
            return self._get_stale_cache_fallback()
        except Exception as e:
            logger.error(f"[GoogleWeatherProvider] Fetch failed: {e}", exc_info=True)
            return self._get_stale_cache_fallback()

    def _parse_hourly_data(self, raw_forecasts: List[Dict]) -> List[GoogleHourlyData]:
        """Parse raw API response into structured hourly data."""
        results: List[GoogleHourlyData] = []

        for item in raw_forecasts:
            try:
                # Parse ISO time (e.g., "2025-12-18T15:00:00Z")
                time_str = item.get("interval", {}).get("startTime", "")
                if not time_str:
                    continue

                # Extract temperature data
                temp_c = self._get_nested(item, ["temperature", "degrees"], 0.0)
                feels_like = self._get_nested(item, ["feelsLikeTemperature", "degrees"], temp_c)
                dewpoint = self._get_nested(item, ["dewPoint", "degrees"], 0.0)

                # Precipitation
                precip_prob = self._get_nested(item, ["precipitation", "probability", "percent"], 0)
                precip_mm = self._get_nested(item, ["precipitation", "qpf", "quantity"], 0.0)

                # Cloud cover and wind
                cloud_cover = self._get_nested(item, ["cloudCover"], 0)
                wind_speed = self._get_nested(item, ["wind", "speed", "value"], 0.0)

                # Condition description
                condition = self._get_nested(item, ["weatherCondition", "description", "text"], "Unknown")
                if not condition or condition == "Unknown":
                    # Fallback to type if description not available
                    condition = item.get("weatherCondition", {}).get("type", "Unknown")

                is_day = item.get("isDaytime", True)

                results.append({
                    "time": time_str,
                    "temp_c": round(float(temp_c), 1),
                    "feels_like_c": round(float(feels_like), 1),
                    "precip_prob": int(precip_prob) if precip_prob else 0,
                    "precip_mm": float(precip_mm) if precip_mm else 0.0,
                    "dewpoint_c": round(float(dewpoint), 1),
                    "cloud_cover": int(cloud_cover) if cloud_cover else 0,
                    "wind_speed_kmh": round(float(wind_speed), 1),
                    "condition": str(condition),
                    "is_daytime": bool(is_day)
                })

            except Exception as e:
                logger.debug(f"[GoogleWeatherProvider] Error parsing hour: {e}")
                continue

        logger.info(f"[GoogleWeatherProvider] Parsed {len(results)} hourly records")
        return results

    def _aggregate_to_daily(self, hourly_data: List[GoogleHourlyData]) -> List[GoogleDailyData]:
        """
        Aggregate hourly data to daily highs/lows.

        IMPORTANT: Uses DIFFERENT day definitions for temps vs precip:
        - TEMPERATURE: Meteorological day (6am-6am) - industry standard
        - PRECIPITATION: Calendar day (midnight-midnight) - matches user expectation

        This prevents overnight rain from incorrectly inflating the previous day's
        precipitation probability (e.g., rain at midnight Wednesday being shown as
        Tuesday's precip).
        """
        logger.info(f"[GoogleWeatherProvider] _aggregate_to_daily called with {len(hourly_data)} hourly records")

        try:
            tz = ZoneInfo(self.TIMEZONE)
        except Exception as e:
            logger.error(f"[GoogleWeatherProvider] Failed to create timezone: {e}")
            return []

        # Separate containers for met-day (temps) and calendar-day (precip)
        daily_temps: Dict[str, List[float]] = {}      # Uses meteorological day
        daily_precip: Dict[str, List[int]] = {}       # Uses CALENDAR day
        daily_conditions: Dict[str, List[str]] = {}   # Uses meteorological day
        processed_count = 0
        error_count = 0

        for hour in hourly_data:
            try:
                time_str = hour['time']
                # Parse UTC time and convert to local
                if 'Z' in time_str:
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00')).astimezone(tz)
                else:
                    dt = datetime.fromisoformat(time_str).astimezone(tz)

                # CALENDAR day for precipitation (midnight-midnight)
                # This matches user expectation: rain at 1am Wed = Wednesday's rain
                calendar_date = dt.strftime('%Y-%m-%d')

                # METEOROLOGICAL day for temps (6am-6am)
                # This is industry standard for hi/lo temp reporting
                if dt.hour < 6:
                    met_day = dt - timedelta(days=1)
                else:
                    met_day = dt
                met_date = met_day.strftime('%Y-%m-%d')

                # Initialize containers
                if met_date not in daily_temps:
                    daily_temps[met_date] = []
                    daily_conditions[met_date] = []
                if calendar_date not in daily_precip:
                    daily_precip[calendar_date] = []

                # Temps and conditions use meteorological day
                daily_temps[met_date].append(hour['temp_c'])
                if hour.get('is_daytime', True):
                    daily_conditions[met_date].append(hour['condition'])

                # Precip uses CALENDAR day
                daily_precip[calendar_date].append(hour['precip_prob'])

                processed_count += 1

            except Exception as e:
                error_count += 1
                logger.warning(f"[GoogleWeatherProvider] Error aggregating hour: {e}")
                continue

        logger.info(f"[GoogleWeatherProvider] Aggregation loop: {processed_count} processed, {error_count} errors, {len(daily_temps)} unique met-days")

        # Build daily results (keyed by meteorological day for temps)
        results: List[GoogleDailyData] = []
        for met_date in sorted(daily_temps.keys()):
            temps = daily_temps[met_date]
            if not temps:
                continue

            high_c = max(temps)
            low_c = min(temps)
            high_f = round(high_c * 9/5 + 32)
            low_f = round(low_c * 9/5 + 32)

            # PRECIP: Use calendar day's max probability
            # This prevents overnight rain attribution to previous day
            precip = max(daily_precip.get(met_date, [0]))

            # Most common daytime condition
            conditions = daily_conditions.get(met_date, ["Unknown"])
            condition = max(set(conditions), key=conditions.count) if conditions else "Unknown"

            results.append({
                "date": met_date,
                "high_c": round(high_c, 1),
                "low_c": round(low_c, 1),
                "high_f": high_f,
                "low_f": low_f,
                "precip_prob": precip,
                "condition": condition
            })

        logger.info(f"[GoogleWeatherProvider] Aggregated to {len(results)} daily records")
        return results

    def _get_nested(self, obj: Dict, path: List[str], default: Any = None) -> Any:
        """Safely get nested dictionary value."""
        current = obj
        for key in path:
            if isinstance(current, dict):
                current = current.get(key, {})
            else:
                return default
        return current if current != {} else default

    async def fetch_daily(self) -> Optional[List[GoogleDailyData]]:
        """
        Convenience method to fetch only daily aggregated data.

        Returns:
            List of GoogleDailyData dicts, or None on failure
        """
        result = await self.fetch_forecast(hours=96)
        if result and 'daily' in result:
            return result['daily']
        return None


if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.DEBUG)

    async def test():
        print("=" * 60)
        print("  GOOGLE WEATHER API PROVIDER TEST (MetNet-3)")
        print("=" * 60)

        provider = GoogleWeatherProvider()

        # Fetch forecast
        print("\n[FETCHING FORECAST]")
        data = await provider.fetch_forecast(hours=48)

        if data:
            print(f"\n[RESULTS] Google Weather Forecast:")
            print("-" * 50)

            # Show hourly sample
            hourly = data.get('hourly', [])
            print(f"\nHourly data: {len(hourly)} records")
            if hourly:
                print(f"  First hour: {hourly[0]}")

            # Show daily
            daily = data.get('daily', [])
            print(f"\nDaily aggregated: {len(daily)} days")
            for day in daily[:5]:
                print(f"  {day['date']}: Hi={day['high_f']}F, Lo={day['low_f']}F, "
                      f"Precip={day['precip_prob']}%, {day['condition']}")
        else:
            print("[FAILED] Could not fetch Google Weather data")

        print("\n" + "=" * 60)

    asyncio.run(test())
