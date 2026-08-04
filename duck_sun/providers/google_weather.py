"""
Google Maps Platform Weather API Provider for Duck Sun Modesto

Powered by Google's "WeatherNext" and "MetNet-3" neural models.
Focus: Hyperlocal precision across the full 240-hour (10 day) forecast window.

API DOCS: https://developers.google.com/maps/documentation/weather

WEIGHTING STRATEGY:
- Weight 8.0 (Primary Source - MetNet-3 Neural Model)
- Superior short-term precision via real-time radar/satellite fusion
- Best for "nowcasting" rather than physics simulations

FORECAST LENGTH (verified against Google's REST reference, Jul 2026):
- `forecast/hours:lookup` accepts `hours` = 1..240 (default 240)
- `pageSize` is 1..24 (default 24), so a full 240-hour pull is 10 pages
- There is NO pricing-tier cap on forecast length: Weather API billing is a
  per-call SKU, so a cheaper plan limits call VOLUME, not forecast horizon.
  The report previously showed only 4 days purely because this provider
  requested hours=96.

MULTI-LOCATION:
- The provider is location-parameterized. Modesto (the forecast subject) is
  the default; Portland, OR is fetched as a separate read-only side reference.

RATE LIMITING:
- Check Google Cloud Console for quota limits
- Implements pagination for full 240-hour forecasts
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
# Default (Modesto) cache path. Each provider instance resolves its own
# `self.cache_file` from its cache_key so locations never share a file.
CACHE_FILE = CACHE_DIR / "google_weather_lkg.json"

# Import SSL helper for Windows certificate store support
try:
    from duck_sun.ssl_helper import get_httpx_ssl_context
except ImportError:
    import ssl as _ssl
    def get_httpx_ssl_context():
        return _ssl.create_default_context()


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

    WEIGHT: 8.0 (Highest - Neural/Satellite Fusion)
    - Best accuracy for 0-240 hour forecasts (strongest in the first 96h)
    - Real-time data fusion vs physics-only models

    Defaults to Modesto, CA. Pass lat/lon/timezone/cache_key to fetch a
    different location (e.g. the Portland, OR side reference) without
    clobbering the Modesto cache.
    """

    # Google Weather API endpoint (Forecast Hours)
    BASE_URL = "https://weather.googleapis.com/v1/forecast/hours:lookup"

    # API limits per Google's REST reference for forecast.hours.lookup
    MAX_FORECAST_HOURS = 240   # 10 days - hard API ceiling, not a plan limit
    MAX_PAGE_SIZE = 24         # Records per page; 240h => 10 pages

    # Modesto, CA coordinates
    LAT = 37.6391
    LON = -120.9969

    # Timezone for Modesto
    TIMEZONE = "America/Los_Angeles"

    def __init__(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        timezone: Optional[str] = None,
        location_name: str = "Modesto, CA",
        cache_key: str = "google_weather",
    ):
        self.lat = self.LAT if lat is None else lat
        self.lon = self.LON if lon is None else lon
        self.timezone = timezone or self.TIMEZONE
        self.location_name = location_name
        self.cache_key = cache_key
        self.cache_file = CACHE_DIR / f"{cache_key}_lkg.json"
        self.log_prefix = f"[GoogleWeatherProvider:{location_name}]"

        logger.info(f"{self.log_prefix} Initializing provider...")
        self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            logger.warning(f"{self.log_prefix} No API Key found in env!")
        else:
            logger.info(f"{self.log_prefix} API key loaded successfully")

        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        logger.debug(f"{self.log_prefix} Cache directory: {CACHE_DIR.absolute()}")

    @staticmethod
    def _unwrap_cache(cache: Dict) -> Dict:
        """Return the forecast payload from either cache layout.

        This provider writes `{timestamp, hourly, daily}` straight to
        outputs/cache/<key>_lkg.json, but CacheManager writes its Last Known
        Good wrapper `{provider, timestamp, data: {...}}` to the SAME path and
        runs later, so the wrapper is usually what's on disk. Readers must
        handle both or they silently see "no cached hours".
        """
        if isinstance(cache.get('data'), dict) and (
            'hourly' in cache['data'] or 'daily' in cache['data']
        ):
            return cache['data']
        return cache

    def _load_cache(self) -> Optional[Dict]:
        """Load cached data if it exists."""
        if not self.cache_file.exists():
            logger.info(f"{self.log_prefix} No cache file found")
            return None

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            cached_time_str = cache.get('timestamp')
            if not cached_time_str:
                logger.warning(f"{self.log_prefix} Cache missing timestamp")
                return None

            cached_time = datetime.fromisoformat(cached_time_str)
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60

            logger.info(f"{self.log_prefix} Cache age: {age_minutes:.1f} minutes")
            return self._unwrap_cache(cache)

        except Exception as e:
            logger.warning(f"{self.log_prefix} Cache load error: {e}")
            return None

    def _save_cache(self, hourly_data: List[GoogleHourlyData], daily_data: List[GoogleDailyData]) -> bool:
        """Save forecast data to cache."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'location': f"{self.lat},{self.lon}",
                'location_name': self.location_name,
                'hourly': hourly_data,
                'daily': daily_data
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)

            logger.info(f"{self.log_prefix} Cache saved: {len(hourly_data)} hourly, {len(daily_data)} daily records")
            return True

        except Exception as e:
            logger.error(f"{self.log_prefix} Cache save failed: {e}")
            return False

    def _merge_with_historical(self, new_hourly: List[GoogleHourlyData]) -> List[GoogleHourlyData]:
        """
        Merge new hourly data with cached historical data for today.

        This preserves earlier hours of today that are no longer in the API response
        (e.g., morning duck curve hours when fetching in the evening).
        """
        tz = ZoneInfo(self.timezone)
        today = datetime.now(tz).strftime('%Y-%m-%d')

        # Load existing cache
        if not self.cache_file.exists():
            return new_hourly

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                old_cache = json.load(f)
            existing_hourly = self._unwrap_cache(old_cache).get('hourly', [])
        except Exception as e:
            logger.debug(f"{self.log_prefix} Could not load cache for merge: {e}")
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
                    logger.debug(f"{self.log_prefix} Preserving historical hour: {time_str}")
            except Exception as e:
                logger.debug(f"{self.log_prefix} Error checking old hour: {e}")
                continue

        if preserved:
            logger.info(f"{self.log_prefix} Preserved {len(preserved)} historical hours for today")

        # Merge: preserved old hours + new hours, sorted by time
        merged = preserved + list(new_hourly)
        merged.sort(key=lambda x: x.get('time', ''))

        return merged

    def _get_stale_cache_fallback(self) -> Optional[Dict]:
        """Return stale cache data as fallback when API fails."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                cache = self._unwrap_cache(raw)
                if cache.get('hourly') or cache.get('daily'):
                    age_str = raw.get('timestamp', 'unknown')
                    logger.warning(f"{self.log_prefix} Returning STALE cache as fallback (cached at: {age_str})")
                    return cache
            except Exception as e:
                logger.error(f"{self.log_prefix} Stale cache fallback failed: {e}")
        return None

    def _parse_google_error(self, resp) -> str:
        """Parse Google API error response for diagnostic logging.

        Google APIs return structured JSON errors with code, message, status,
        and details[].reason fields. This extracts them into a loggable string
        so operators can immediately diagnose 403/401 failures without needing
        to manually curl the API.
        """
        try:
            body = resp.json()
            error = body.get("error", {})
            code = error.get("code", resp.status_code)
            message = error.get("message", "No message")
            status = error.get("status", "UNKNOWN")

            # Extract reason(s) from details array (e.g. SERVICE_DISABLED, BILLING_DISABLED)
            details = error.get("details", [])
            detail_reasons = []
            for d in details:
                reason = d.get("reason", "")
                if reason:
                    detail_reasons.append(reason)

            detail_str = f", reasons=[{', '.join(detail_reasons)}]" if detail_reasons else ""
            return f"code={code}, status={status}, message={message}{detail_str}"
        except Exception:
            # Response isn't JSON - fall back to truncated raw text
            return f"raw_response={resp.text[:500]}"

    async def fetch_forecast(self, hours: int = MAX_FORECAST_HOURS) -> Optional[Dict[str, Any]]:
        """
        Fetch hourly forecast from Google Weather API.

        Args:
            hours: Number of hours to fetch (default 240 = 10 days, the API max).
                   Values above 240 are clamped; Google rejects them outright.

        Returns:
            Dict with 'hourly' and 'daily' keys containing forecast data,
            or None on failure
        """
        if hours > self.MAX_FORECAST_HOURS:
            logger.warning(
                f"{self.log_prefix} Requested {hours}h exceeds API max "
                f"{self.MAX_FORECAST_HOURS}h - clamping"
            )
            hours = self.MAX_FORECAST_HOURS
        hours = max(1, hours)

        if not self.api_key:
            logger.warning(f"{self.log_prefix} Cannot fetch - no API key")
            cache = self._get_stale_cache_fallback()
            return cache

        logger.info(f"{self.log_prefix} Fetching {hours} hours from Google Weather API...")

        params = {
            "key": self.api_key,
            "location.latitude": self.lat,
            "location.longitude": self.lon,
            "hours": hours,           # Total horizon - API paginates at 24hrs/page
            "pageSize": self.MAX_PAGE_SIZE,  # Ask for the biggest page allowed (24)
            "languageCode": "en-US",
            "unitsSystem": "METRIC",
        }

        try:
            async with httpx.AsyncClient(timeout=30.0, verify=get_httpx_ssl_context()) as client:
                all_forecasts = []
                next_page_token = None
                page_count = 0
                # pageSize caps at 24, so 240h needs 10 pages; +2 is slack in case
                # Google returns short pages.
                max_pages = (hours // self.MAX_PAGE_SIZE) + 2

                # Fetch loop for pagination
                while len(all_forecasts) < hours and page_count < max_pages:
                    if next_page_token:
                        params["pageToken"] = next_page_token
                    elif "pageToken" in params:
                        del params["pageToken"]

                    logger.debug(f"{self.log_prefix} Fetching page {page_count + 1}...")
                    resp = await client.get(self.BASE_URL, params=params)

                    if resp.status_code == 401:
                        error_info = self._parse_google_error(resp)
                        logger.error(f"{self.log_prefix} 401 Unauthorized: {error_info}")
                        logger.error(f"{self.log_prefix} ACTION: Verify GOOGLE_MAPS_API_KEY is valid and not expired")
                        return self._get_stale_cache_fallback()

                    if resp.status_code == 403:
                        error_info = self._parse_google_error(resp)
                        logger.error(f"{self.log_prefix} 403 Forbidden: {error_info}")

                        # Log actionable guidance based on Google error status
                        if "PERMISSION_DENIED" in error_info:
                            logger.error(f"{self.log_prefix} ACTION: Enable 'Weather API' in Google Cloud Console -> APIs & Services")
                        elif "RESOURCE_EXHAUSTED" in error_info:
                            logger.error(f"{self.log_prefix} ACTION: Quota exceeded - check Cloud Console quotas or wait for reset")
                        elif "billing" in error_info.lower():
                            logger.error(f"{self.log_prefix} ACTION: Enable billing on the Google Cloud project")
                        else:
                            logger.error(f"{self.log_prefix} ACTION: Check API key restrictions in Cloud Console -> Credentials")

                        return self._get_stale_cache_fallback()

                    if resp.status_code != 200:
                        error_info = self._parse_google_error(resp)
                        logger.error(f"{self.log_prefix} API Error {resp.status_code}: {error_info}")
                        return self._get_stale_cache_fallback()

                    data = resp.json()
                    page_forecasts = data.get("forecastHours", [])
                    all_forecasts.extend(page_forecasts)
                    page_count += 1

                    next_page_token = data.get("nextPageToken")
                    if not next_page_token:
                        break

                logger.info(f"{self.log_prefix} Received {len(all_forecasts)} hourly records ({page_count} pages)")

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
            logger.error(f"{self.log_prefix} Request timed out")
            return self._get_stale_cache_fallback()
        except httpx.RequestError as e:
            logger.error(f"{self.log_prefix} Request error: {e}")
            return self._get_stale_cache_fallback()
        except Exception as e:
            logger.error(f"{self.log_prefix} Fetch failed: {e}", exc_info=True)
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
                logger.debug(f"{self.log_prefix} Error parsing hour: {e}")
                continue

        logger.info(f"{self.log_prefix} Parsed {len(results)} hourly records")
        return results

    def _aggregate_to_daily(self, hourly_data: List[GoogleHourlyData]) -> List[GoogleDailyData]:
        """
        Aggregate hourly data to daily highs/lows.

        Uses calendar day (midnight-midnight) for all aggregations:
        temperatures, precipitation, and conditions.
        """
        logger.info(f"{self.log_prefix} _aggregate_to_daily called with {len(hourly_data)} hourly records")

        try:
            tz = ZoneInfo(self.timezone)
        except Exception as e:
            logger.error(f"{self.log_prefix} Failed to create timezone: {e}")
            return []

        # All containers use calendar day (midnight-midnight)
        daily_temps: Dict[str, List[float]] = {}
        daily_precip: Dict[str, List[int]] = {}
        daily_conditions: Dict[str, List[str]] = {}
        daily_max_hour: Dict[str, int] = {}           # Track max local hour per day
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

                # Calendar day for all aggregations (midnight-midnight)
                cal_date = dt.strftime('%Y-%m-%d')

                # Initialize containers
                if cal_date not in daily_temps:
                    daily_temps[cal_date] = []
                    daily_conditions[cal_date] = []
                    daily_max_hour[cal_date] = 0
                if cal_date not in daily_precip:
                    daily_precip[cal_date] = []

                daily_temps[cal_date].append(hour['temp_c'])
                daily_max_hour[cal_date] = max(daily_max_hour[cal_date], dt.hour)
                if hour.get('is_daytime', True):
                    daily_conditions[cal_date].append(hour['condition'])

                daily_precip[cal_date].append(hour['precip_prob'])

                processed_count += 1

            except Exception as e:
                error_count += 1
                logger.warning(f"{self.log_prefix} Error aggregating hour: {e}")
                continue

        logger.info(f"{self.log_prefix} Aggregation loop: {processed_count} processed, {error_count} errors, {len(daily_temps)} unique days")

        # Build daily results
        results: List[GoogleDailyData] = []
        for date_key in sorted(daily_temps.keys()):
            temps = daily_temps[date_key]
            if not temps:
                continue

            # Skip partial days that lack afternoon data — the "high" would just
            # be a morning temp, not the actual daytime peak. This happens at the
            # edges of the 240-hour API window (first/last day).
            max_hour = daily_max_hour.get(date_key, 0)
            if max_hour < 14:
                logger.info(f"{self.log_prefix} Skipping partial day {date_key} (max local hour={max_hour}, need >=14 for reliable high)")
                continue

            high_c = max(temps)
            low_c = min(temps)
            high_f = round(high_c * 9/5 + 32)
            low_f = round(low_c * 9/5 + 32)

            precip = max(daily_precip.get(date_key, [0]))

            # Most common daytime condition
            conditions = daily_conditions.get(date_key, ["Unknown"])
            condition = max(set(conditions), key=conditions.count) if conditions else "Unknown"

            results.append({
                "date": date_key,
                "high_c": round(high_c, 1),
                "low_c": round(low_c, 1),
                "high_f": high_f,
                "low_f": low_f,
                "precip_prob": precip,
                "condition": condition
            })

        logger.info(f"{self.log_prefix} Aggregated to {len(results)} daily records")
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

    async def fetch_daily(self, hours: int = MAX_FORECAST_HOURS) -> Optional[List[GoogleDailyData]]:
        """
        Convenience method to fetch only daily aggregated data.

        Args:
            hours: Forecast horizon to pull before aggregating (default 240 = 10 days)

        Returns:
            List of GoogleDailyData dicts, or None on failure
        """
        result = await self.fetch_forecast(hours=hours)
        if result and 'daily' in result:
            return result['daily']
        return None


# Portland, OR - side reference only. These temps are displayed on their own
# row and deliberately never enter the Modesto weighted consensus.
PORTLAND_LAT = 45.5152
PORTLAND_LON = -122.6784
PORTLAND_TIMEZONE = "America/Los_Angeles"


class GooglePortlandProvider(GoogleWeatherProvider):
    """Google Weather (MetNet-3) for Portland, OR.

    Portland shares Modesto's Pacific timezone, so calendar-day aggregation
    lines the Portland row up column-for-column with the Modesto grid without
    any date shifting.

    Uses its own cache key so a Portland fetch can never overwrite the
    Modesto Last Known Good data (or vice versa).
    """

    def __init__(self):
        super().__init__(
            lat=PORTLAND_LAT,
            lon=PORTLAND_LON,
            timezone=PORTLAND_TIMEZONE,
            location_name="Portland, OR",
            cache_key="google_portland",
        )


if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.DEBUG)

    async def _show(provider: GoogleWeatherProvider, hours: int):
        print("=" * 60)
        print(f"  GOOGLE WEATHER API TEST (MetNet-3) - {provider.location_name}")
        print("=" * 60)

        print(f"\n[FETCHING {hours}h FORECAST]")
        data = await provider.fetch_forecast(hours=hours)

        if not data:
            print("[FAILED] Could not fetch Google Weather data")
            return

        print(f"\n[RESULTS] {provider.location_name}:")
        print("-" * 50)

        hourly = data.get('hourly', [])
        print(f"\nHourly data: {len(hourly)} records")
        if hourly:
            print(f"  First hour: {hourly[0]}")

        daily = data.get('daily', [])
        print(f"\nDaily aggregated: {len(daily)} days")
        for day in daily:
            print(f"  {day['date']}: Hi={day['high_f']}F, Lo={day['low_f']}F, "
                  f"Precip={day['precip_prob']}%, {day['condition']}")

        print("\n" + "=" * 60)

    async def test():
        # Full 240h pull for both locations - this is what the report now uses.
        await _show(GoogleWeatherProvider(), GoogleWeatherProvider.MAX_FORECAST_HOURS)
        print()
        await _show(GooglePortlandProvider(), GoogleWeatherProvider.MAX_FORECAST_HOURS)

    asyncio.run(test())
