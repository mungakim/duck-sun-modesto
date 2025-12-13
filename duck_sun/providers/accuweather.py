"""
AccuWeather Provider for Duck Sun Modesto

Fetches 5-Day Daily Forecasts.
Uses Key: 327145 (Modesto, CA) - CORRECTED from 332066
Source: https://www.accuweather.com/en/us/modesto/95354/weather-forecast/327145

RATE LIMITING:
- Free Tier: 50 calls/day
- Cache TTL: 1 hour (prevents redundant API calls)
- Cache Location: outputs/accuweather_cache.json
"""

import httpx
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, TypedDict

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "accuweather_cache.json"
CACHE_TTL_HOURS = 1  # Cache valid for 1 hour


class AccuWeatherDay(TypedDict):
    date: str
    high_c: float
    low_c: float
    high_f: float  # Native Fahrenheit from API (no conversion rounding)
    low_f: float   # Native Fahrenheit from API (no conversion rounding)
    condition: str
    precip_prob: int


class AccuWeatherProvider:
    """
    Provider for AccuWeather data.
    Restricted to 50 calls/day (Free Tier).
    
    CACHE GUARDRAIL:
    - Caches forecast data for 1 hour to prevent quota exhaustion
    - Cache stored in outputs/accuweather_cache.json
    - Automatically serves cached data if fetched within TTL
    """
    
    # CORRECT Modesto, CA Location Key
    # Source: https://www.accuweather.com/en/us/modesto/95354/weather-forecast/327145
    LOCATION_KEY = "327145"
    BASE_URL = "https://dataservice.accuweather.com"

    def __init__(self):
        logger.info("[AccuWeatherProvider] Initializing provider...")
        self.api_key = os.getenv("ACCUWEATHER_API_KEY")
        if not self.api_key:
            logger.warning("[AccuWeatherProvider] No API Key found in env!")
        else:
            logger.info("[AccuWeatherProvider] API key loaded successfully")
        
        # Ensure cache directory exists
        CACHE_DIR.mkdir(exist_ok=True)
        logger.debug(f"[AccuWeatherProvider] Cache directory: {CACHE_DIR.absolute()}")
    
    def _load_cache(self) -> Optional[dict]:
        """
        Load cached data if it exists and is within TTL.
        
        Returns:
            dict with 'timestamp' and 'data' keys, or None if cache invalid/missing
        """
        if not CACHE_FILE.exists():
            logger.info("[AccuWeatherProvider] No cache file found")
            return None
        
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            cached_time_str = cache.get('timestamp')
            if not cached_time_str:
                logger.warning("[AccuWeatherProvider] Cache missing timestamp, invalidating")
                return None
            
            cached_time = datetime.fromisoformat(cached_time_str)
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60
            
            logger.info(f"[AccuWeatherProvider] Cache age: {age_minutes:.1f} minutes")
            
            if age <= timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"[AccuWeatherProvider] [OK] Cache VALID (TTL: {CACHE_TTL_HOURS}h, Age: {age_minutes:.1f}m)")
                return cache
            else:
                logger.info(f"[AccuWeatherProvider] Cache EXPIRED (TTL: {CACHE_TTL_HOURS}h, Age: {age_minutes:.1f}m)")
                return None
                
        except json.JSONDecodeError as e:
            logger.warning(f"[AccuWeatherProvider] Cache corrupted: {e}")
            return None
        except Exception as e:
            logger.error(f"[AccuWeatherProvider] Cache load error: {e}")
            return None
    
    def _save_cache(self, data: List[AccuWeatherDay]) -> bool:
        """
        Save forecast data to cache with timestamp.
        
        Args:
            data: List of forecast day dictionaries
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'location_key': self.LOCATION_KEY,
                'ttl_hours': CACHE_TTL_HOURS,
                'data': data
            }
            
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)
            
            logger.info(f"[AccuWeatherProvider] Cache saved: {len(data)} days -> {CACHE_FILE}")
            return True
            
        except Exception as e:
            logger.error(f"[AccuWeatherProvider] Cache save failed: {e}")
            return False
    
    def get_cache_info(self) -> dict:
        """
        Get information about the current cache state.
        
        Returns:
            dict with cache status, age, and expiration info
        """
        if not CACHE_FILE.exists():
            return {'exists': False, 'valid': False, 'age_minutes': None}
        
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60
            is_valid = age <= timedelta(hours=CACHE_TTL_HOURS)
            expires_in = (timedelta(hours=CACHE_TTL_HOURS) - age).total_seconds() / 60 if is_valid else 0
            
            return {
                'exists': True,
                'valid': is_valid,
                'age_minutes': round(age_minutes, 1),
                'expires_in_minutes': round(max(0, expires_in), 1),
                'cached_at': cache.get('timestamp'),
                'record_count': len(cache.get('data', []))
            }
        except Exception:
            return {'exists': True, 'valid': False, 'age_minutes': None, 'error': 'Parse failed'}

    async def fetch_forecast(self, force_refresh: bool = False) -> Optional[List[AccuWeatherDay]]:
        """
        Fetch 5-Day Daily Forecast with 1-hour cache guardrail.
        
        Args:
            force_refresh: If True, bypass cache and fetch fresh data (use sparingly!)
            
        Returns:
            List of AccuWeatherDay dicts, or None on failure
            
        CACHE BEHAVIOR:
        - Checks cache first (unless force_refresh=True)
        - If cache valid (< 1 hour old), returns cached data WITHOUT API call
        - If cache expired/missing, fetches from API and updates cache
        - Protects against 50 calls/day quota exhaustion
        """
        # STEP 1: Check cache first (unless forced refresh)
        if not force_refresh:
            cache = self._load_cache()
            if cache and cache.get('data'):
                cache_info = self.get_cache_info()
                logger.info(f"[AccuWeatherProvider] CACHE HIT - Returning cached data "
                           f"(Age: {cache_info.get('age_minutes', '?')}m, "
                           f"Expires in: {cache_info.get('expires_in_minutes', '?')}m)")
                logger.info(f"[AccuWeatherProvider] API call AVOIDED - Quota protected!")
                return cache['data']
            else:
                logger.info("[AccuWeatherProvider] Cache miss - will fetch from API")
        else:
            logger.warning("[AccuWeatherProvider] [!] FORCE REFRESH - Bypassing cache (quota impact!)")
        
        # STEP 2: Check API key
        if not self.api_key:
            logger.warning("[AccuWeatherProvider] Cannot fetch - no API key")
            # Try to return stale cache as fallback
            if CACHE_FILE.exists():
                try:
                    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                    if cache.get('data'):
                        logger.info("[AccuWeatherProvider] Returning STALE cache (no API key fallback)")
                        return cache['data']
                except Exception:
                    pass
            return None

        logger.info(f"[AccuWeatherProvider] [API] Fetching 5-Day Forecast for Location Key {self.LOCATION_KEY} (Modesto, CA)...")
        logger.info("[AccuWeatherProvider] API CALL - This counts against 50/day quota!")
        
        url = f"{self.BASE_URL}/forecasts/v1/daily/5day/{self.LOCATION_KEY}"
        params = {
            "apikey": self.api_key,
            "metric": "false",  # Request native Fahrenheit to avoid conversion rounding
            "details": "true" 
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                logger.debug(f"[AccuWeatherProvider] GET {url}")
                resp = await client.get(url, params=params)
                
                if resp.status_code == 503:
                    logger.warning("[AccuWeatherProvider] Quota exceeded (50/day limit)")
                    # Return stale cache as fallback
                    return self._get_stale_cache_fallback()
                if resp.status_code == 401:
                    logger.warning("[AccuWeatherProvider] Unauthorized - check API key")
                    return None
                if resp.status_code != 200:
                    logger.warning(f"[AccuWeatherProvider] HTTP {resp.status_code}: {resp.text[:100]}")
                    return self._get_stale_cache_fallback()

                data = resp.json()
                daily_forecasts = data.get("DailyForecasts", [])
                
                logger.info(f"[AccuWeatherProvider] Parsing {len(daily_forecasts)} daily forecasts...")
                
                results: List[AccuWeatherDay] = []
                for day in daily_forecasts:
                    date_str = day.get("Date", "")[:10]
                    # Get native Fahrenheit values (no conversion rounding)
                    high_f = day.get("Temperature", {}).get("Maximum", {}).get("Value")
                    low_f = day.get("Temperature", {}).get("Minimum", {}).get("Value")
                    
                    # Convert F to C: C = (F - 32) * 5/9
                    high_c = (high_f - 32) * 5/9 if high_f is not None else 0.0
                    low_c = (low_f - 32) * 5/9 if low_f is not None else 0.0
                    
                    day_part = day.get("Day", {})
                    precip = day_part.get("PrecipitationProbability", 0)
                    cond = day_part.get("IconPhrase", "Unknown")

                    logger.debug(f"[AccuWeatherProvider] {date_str}: Hi={high_f}F ({high_c:.1f}C), "
                               f"Lo={low_f}F ({low_c:.1f}C), Precip={precip}%, Cond={cond}")

                    results.append({
                        "date": date_str,
                        "high_c": round(high_c, 2),
                        "low_c": round(low_c, 2),
                        "high_f": float(high_f) if high_f is not None else 0.0,
                        "low_f": float(low_f) if low_f is not None else 0.0,
                        "precip_prob": int(precip),
                        "condition": cond
                    })
                
                logger.info(f"[AccuWeatherProvider] [OK] Retrieved {len(results)} daily records from API")
                
                # STEP 3: Save to cache
                self._save_cache(results)
                
                return results

        except httpx.TimeoutException:
            logger.error("[AccuWeatherProvider] Request timed out")
            return self._get_stale_cache_fallback()
        except httpx.RequestError as e:
            logger.error(f"[AccuWeatherProvider] Request error: {e}")
            return self._get_stale_cache_fallback()
        except Exception as e:
            logger.error(f"[AccuWeatherProvider] Fetch failed: {e}", exc_info=True)
            return self._get_stale_cache_fallback()
    
    def _get_stale_cache_fallback(self) -> Optional[List[AccuWeatherDay]]:
        """
        Return stale cache data as fallback when API fails.
        Better to have old data than no data.
        """
        if CACHE_FILE.exists():
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                if cache.get('data'):
                    age_str = cache.get('timestamp', 'unknown')
                    logger.warning(f"[AccuWeatherProvider] [!] Returning STALE cache as fallback (cached at: {age_str})")
                    return cache['data']
            except Exception as e:
                logger.error(f"[AccuWeatherProvider] Stale cache fallback failed: {e}")
        return None


if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv
    
    load_dotenv()
    logging.basicConfig(level=logging.DEBUG)
    
    async def test():
        print("=" * 60)
        print("  ACCUWEATHER PROVIDER TEST (with Cache Guardrail)")
        print("=" * 60)
        
        provider = AccuWeatherProvider()
        
        # Show cache status
        print("\n[CACHE STATUS]")
        cache_info = provider.get_cache_info()
        print(f"  Cache exists: {cache_info.get('exists', False)}")
        print(f"  Cache valid:  {cache_info.get('valid', False)}")
        if cache_info.get('age_minutes') is not None:
            print(f"  Cache age:    {cache_info.get('age_minutes')} minutes")
            print(f"  Expires in:   {cache_info.get('expires_in_minutes')} minutes")
            print(f"  Records:      {cache_info.get('record_count', 0)}")
        
        # Fetch forecast (will use cache if valid)
        print("\n[FETCHING FORECAST]")
        data = await provider.fetch_forecast()
        
        if data:
            print(f"\n[RESULTS] AccuWeather 5-Day Forecast ({len(data)} days):")
            print("-" * 50)
            for day in data:
                # Convert C to F for display
                high_f = day['high_c'] * 9/5 + 32
                low_f = day['low_c'] * 9/5 + 32
                print(f"  {day['date']}: Hi={day['high_c']:.1f}C ({high_f:.0f}F), "
                      f"Lo={day['low_c']:.1f}C ({low_f:.0f}F), "
                      f"Precip={day['precip_prob']}%, {day['condition']}")
        else:
            print("[FAILED] Could not fetch AccuWeather data")
        
        # Show updated cache status
        print("\n[POST-FETCH CACHE STATUS]")
        cache_info = provider.get_cache_info()
        print(f"  Cache valid:  {cache_info.get('valid', False)}")
        if cache_info.get('age_minutes') is not None:
            print(f"  Expires in:   {cache_info.get('expires_in_minutes')} minutes")
        
        print("\n" + "=" * 60)
        print("  NOTE: Run again within 1 hour to see CACHE HIT behavior")
        print("=" * 60)
    
    asyncio.run(test())
