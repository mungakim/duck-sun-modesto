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

    def _build_results_from_forecast(self, forecast: dict) -> Optional[List[WeatherComDay]]:
        """Build WeatherComDay results from parsed forecast JSON structure."""
        try:
            # Extract daily data arrays - try multiple possible key names
            days_of_week = forecast.get('dayOfWeek', forecast.get('dayofWeek', []))
            max_temps = forecast.get('temperatureMax', forecast.get('calendarDayTemperatureMax', []))
            min_temps = forecast.get('temperatureMin', forecast.get('calendarDayTemperatureMin', []))

            # Log what we found
            logger.info(f"[WeatherComProvider] days_of_week: {len(days_of_week)} items, first 3: {days_of_week[:3]}")
            logger.info(f"[WeatherComProvider] max_temps: {len(max_temps)} items, first 3: {max_temps[:3]}")
            logger.info(f"[WeatherComProvider] min_temps: {len(min_temps)} items, first 3: {min_temps[:3]}")

            # Precipitation is in daypart structure (alternating day/night)
            daypart = forecast.get('daypart', None)

            # Handle daypart as list (common format)
            if isinstance(daypart, list) and daypart:
                daypart = daypart[0]  # First element contains the data

            # Try to get precip from daypart
            precip_chances = []
            if isinstance(daypart, dict):
                precip_chances = daypart.get('precipChance', [])
                logger.info(f"[WeatherComProvider] precip from daypart: {len(precip_chances)} items, first 6: {precip_chances[:6]}")
            else:
                # Try direct precipChance in forecast (some structures)
                precip_chances = forecast.get('precipChance', [])
                if precip_chances:
                    logger.info(f"[WeatherComProvider] precip from forecast: {len(precip_chances)} items")

            if not days_of_week or not max_temps or not min_temps:
                logger.warning(f"[WeatherComProvider] Missing required arrays - days:{len(days_of_week)}, max:{len(max_temps)}, min:{len(min_temps)}")
                logger.warning(f"[WeatherComProvider] Available keys: {list(forecast.keys())[:20]}")
                return None

            return self._build_results_from_arrays(days_of_week, max_temps, min_temps, precip_chances)

        except Exception as e:
            logger.error(f"[WeatherComProvider] Error building results from forecast: {e}", exc_info=True)
            return None

    def _build_results_from_arrays(
        self,
        days_of_week: List,
        max_temps: List,
        min_temps: List,
        precip_chances: List
    ) -> Optional[List[WeatherComDay]]:
        """Build WeatherComDay results from extracted arrays."""
        logger.info(f"[WeatherComProvider] Building results: {len(days_of_week)} days, precip raw: {precip_chances[:10] if precip_chances else []}")

        # Build daily precip from day/night pairs (take max of each pair)
        daily_precip = []
        for i in range(0, len(precip_chances), 2):
            day_p = precip_chances[i] if i < len(precip_chances) and precip_chances[i] is not None else 0
            night_p = precip_chances[i + 1] if i + 1 < len(precip_chances) and precip_chances[i + 1] is not None else 0
            daily_precip.append(max(day_p, night_p))

        logger.info(f"[WeatherComProvider] Daily precip (max of day/night): {daily_precip[:10]}")

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

        logger.info(f"[WeatherComProvider] [OK] Built {len(results)} forecast days")
        if results:
            self._save_cache(results)
        return results if results else None

    def _extract_from_next_data(self, soup: 'BeautifulSoup') -> Optional[dict]:
        """
        Extract forecast data from Weather.com's __NEXT_DATA__ JSON.

        Weather.com is a Next.js app that embeds all data in a script tag
        with id="__NEXT_DATA__". The forecast data is deeply nested.
        """
        # Find the __NEXT_DATA__ script tag
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if not next_data_script or not next_data_script.string:
            logger.warning("[WeatherComProvider] No __NEXT_DATA__ script tag found")
            return None

        try:
            data = json.loads(next_data_script.string)
            logger.info(f"[WeatherComProvider] Parsed __NEXT_DATA__ - top keys: {list(data.keys())}")

            # Navigate to forecast data - structure varies but typically:
            # props.pageProps.*** contains the forecast
            props = data.get('props', {})
            page_props = props.get('pageProps', {})

            logger.info(f"[WeatherComProvider] pageProps keys: {list(page_props.keys())[:15]}")

            # Try different possible locations for forecast data
            forecast = None

            # Location 1: pageProps.forecast (direct)
            if 'forecast' in page_props:
                forecast = page_props['forecast']
                logger.info("[WeatherComProvider] Found at pageProps.forecast")

            # Location 2: pageProps.data.forecast
            if not forecast and 'data' in page_props:
                pdata = page_props['data']
                if isinstance(pdata, dict):
                    logger.debug(f"[WeatherComProvider] pageProps.data keys: {list(pdata.keys())[:10]}")
                    forecast = pdata.get('forecast', {})
                    if forecast:
                        logger.info("[WeatherComProvider] Found at pageProps.data.forecast")

            # Location 3: pageProps.pageData.forecast
            if not forecast and 'pageData' in page_props:
                page_data = page_props['pageData']
                if isinstance(page_data, dict):
                    logger.debug(f"[WeatherComProvider] pageData keys: {list(page_data.keys())[:10]}")
                    forecast = page_data.get('forecast', {})
                    if forecast:
                        logger.info("[WeatherComProvider] Found at pageProps.pageData.forecast")

            # Location 4: Look in initialState or similar
            if not forecast and 'initialState' in page_props:
                init_state = page_props['initialState']
                if isinstance(init_state, dict):
                    logger.debug(f"[WeatherComProvider] initialState keys: {list(init_state.keys())[:10]}")
                    # Try dal.getSunV3DailyForecastWithHeadersUrlConfig
                    dal = init_state.get('dal', {})
                    if dal:
                        logger.debug(f"[WeatherComProvider] dal keys: {list(dal.keys())[:5]}")
                        # Search for any key containing 'DailyForecast'
                        for key in dal.keys():
                            if 'DailyForecast' in key or 'dailyForecast' in key:
                                forecast_data = dal[key]
                                if isinstance(forecast_data, dict) and 'data' in forecast_data:
                                    forecast = forecast_data['data']
                                    logger.info(f"[WeatherComProvider] Found at initialState.dal.{key}.data")
                                    break

            # Location 5: Recursive search as fallback
            if not forecast:
                logger.info("[WeatherComProvider] Trying recursive search...")
                forecast = self._find_forecast_in_json(data)
                if forecast:
                    logger.info("[WeatherComProvider] Found via recursive search")

            if forecast:
                if isinstance(forecast, dict):
                    logger.info(f"[WeatherComProvider] Forecast keys: {list(forecast.keys())[:15]}")
                return forecast
            else:
                logger.warning("[WeatherComProvider] No forecast data found in __NEXT_DATA__")

        except json.JSONDecodeError as e:
            logger.warning(f"[WeatherComProvider] Failed to parse __NEXT_DATA__: {e}")
        except Exception as e:
            logger.warning(f"[WeatherComProvider] Error extracting __NEXT_DATA__: {e}", exc_info=True)

        return None

    def _find_forecast_in_json(self, data, depth: int = 0, path: str = "root") -> Optional[dict]:
        """Recursively search for forecast data containing temperatureMax."""
        if depth > 15:  # Prevent infinite recursion (increased depth for Weather.com)
            return None

        if isinstance(data, dict):
            # Check if this dict has the forecast arrays we need
            if 'temperatureMax' in data and 'temperatureMin' in data:
                logger.info(f"[WeatherComProvider] Found temperatureMax at path: {path}")
                return data

            # Also check for daypart structure with precipChance
            if 'daypart' in data and isinstance(data['daypart'], list):
                daypart = data['daypart'][0] if data['daypart'] else {}
                if isinstance(daypart, dict) and 'precipChance' in daypart:
                    logger.info(f"[WeatherComProvider] Found daypart with precipChance at path: {path}")
                    # Merge daypart data with daily data
                    return {**data, 'daypart': daypart}

            # Recurse into child dicts
            for key, value in data.items():
                result = self._find_forecast_in_json(value, depth + 1, f"{path}.{key}")
                if result:
                    return result

        elif isinstance(data, list) and len(data) > 0:
            # Only check first few items to avoid excessive searching
            for i, item in enumerate(data[:3]):
                result = self._find_forecast_in_json(item, depth + 1, f"{path}[{i}]")
                if result:
                    return result

        return None

    def _fetch_via_scraping(self) -> Optional[List[WeatherComDay]]:
        """
        Fetch forecast via web scraping with embedded JSON extraction.

        Weather.com is a Next.js app - extracts forecast from __NEXT_DATA__ JSON.
        Falls back to regex extraction if __NEXT_DATA__ parsing fails.
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

            # METHOD 1: Try __NEXT_DATA__ JSON parsing (most reliable for Next.js sites)
            forecast = self._extract_from_next_data(soup)
            if forecast:
                return self._build_results_from_forecast(forecast)

            # METHOD 2: Try regex extraction from any script with forecast keywords
            scripts = soup.find_all('script')
            forecast_json = None

            for script in scripts:
                if script.string and 'temperatureMax' in script.string and 'precipChance' in script.string:
                    forecast_json = script.string
                    logger.info("[WeatherComProvider] Found script with forecast keywords, trying regex")
                    break

            if forecast_json:
                # Log a sample of the script content for debugging
                sample_start = forecast_json.find('temperatureMax')
                if sample_start > 0:
                    logger.info(f"[WeatherComProvider] Script sample near temperatureMax: {forecast_json[sample_start:sample_start+300]}")

                # Try to find window.__data or similar embedded JSON
                window_data_patterns = [
                    r'window\.__data\s*=\s*(\{.+\});',
                    r'window\.weatherData\s*=\s*(\{.+\});',
                    r'self\.__next_f\.push\(\[.*?,\s*"([^"]*temperatureMax[^"]*)"\]',
                ]
                for pattern in window_data_patterns:
                    match = re.search(pattern, forecast_json, re.DOTALL)
                    if match:
                        try:
                            data_str = match.group(1)
                            # Handle escaped JSON in Next.js streaming format
                            if '\\' in data_str:
                                data_str = data_str.encode().decode('unicode_escape')
                            data_obj = json.loads(data_str)
                            logger.info(f"[WeatherComProvider] Found window data with pattern: {pattern[:30]}")
                            forecast = self._find_forecast_in_json(data_obj)
                            if forecast:
                                return self._build_results_from_forecast(forecast)
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            logger.debug(f"[WeatherComProvider] Failed to parse window data: {e}")

                # Try to parse embedded JSON objects - Weather.com may embed data as JSON blob
                # Look for patterns like {"temperatureMax":[...], ...}
                import re
                json_match = re.search(r'\{[^{}]*"temperatureMax"\s*:\s*\[[^\]]+\][^{}]*\}', forecast_json)
                if json_match:
                    try:
                        forecast_obj = json.loads(json_match.group(0))
                        logger.info(f"[WeatherComProvider] Parsed JSON object with keys: {list(forecast_obj.keys())}")
                        return self._build_results_from_forecast(forecast_obj)
                    except json.JSONDecodeError:
                        logger.debug("[WeatherComProvider] Failed to parse JSON object")

                # Try multiple regex patterns - Weather.com JSON might have different formats
                # Pattern A: Standard JSON arrays (no spaces)
                days_of_week = self._extract_json_array(r'"dayOfWeek":\[([^\]]+)\]', forecast_json, is_numeric=False)
                if not days_of_week:
                    # Pattern B: JSON with spaces after colon
                    days_of_week = self._extract_json_array(r'"dayOfWeek":\s*\[([^\]]+)\]', forecast_json, is_numeric=False)

                max_temps = self._extract_json_array(r'"temperatureMax":\[([^\]]+)\]', forecast_json)
                if not max_temps:
                    max_temps = self._extract_json_array(r'"temperatureMax":\s*\[([^\]]+)\]', forecast_json)

                min_temps = self._extract_json_array(r'"temperatureMin":\[([^\]]+)\]', forecast_json)
                if not min_temps:
                    min_temps = self._extract_json_array(r'"temperatureMin":\s*\[([^\]]+)\]', forecast_json)

                precip_chances = self._extract_json_array(r'"precipChance":\[([^\]]+)\]', forecast_json)
                if not precip_chances:
                    precip_chances = self._extract_json_array(r'"precipChance":\s*\[([^\]]+)\]', forecast_json)

                logger.info(f"[WeatherComProvider] Regex extracted: days={len(days_of_week)}, max={len(max_temps)}, min={len(min_temps)}, precip={len(precip_chances)}")

                if days_of_week and max_temps and min_temps:
                    return self._build_results_from_arrays(days_of_week, max_temps, min_temps, precip_chances)

            # METHOD 3: Fall back to HTML element parsing (no precip available)
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
        print("-" * 60)
        print(f"{'Date':<12} | {'Day':<5} | {'High':<6} | {'Low':<6} | {'Precip':<6}")
        print("-" * 60)
        for day in data:
            print(f"{day['date']:<12} | {day['day_name']:<5} | {day['high_f']:.0f}F    | {day['low_f']:.0f}F    | {day['precip_prob']}%")
    else:
        print("[FAILED] Could not fetch Weather.com data")

    print("\n" + "=" * 60)
