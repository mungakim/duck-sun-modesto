"""
NOAA (National Oceanic and Atmospheric Administration) Provider for Duck Sun Modesto

Fetches official US government temperature forecasts from api.weather.gov.
UPGRADE: Uses the 'forecast' endpoint (Periods) for daily High/Low
to match the NOAA weather.gov website's human-curated numbers.
"""

import httpx
import logging
from datetime import datetime
from typing import List, Optional, TypedDict, Dict, Any

logger = logging.getLogger(__name__)


class NOAATemperature(TypedDict):
    time: str
    temp_c: float


class NOAATextForecast(TypedDict):
    name: str
    detailedForecast: str


class NOAAPeriod(TypedDict):
    name: str
    startTime: str
    isDaytime: bool
    temperature: int
    temperatureUnit: str
    detailedForecast: str


class NOAAProvider:
    """
    Provider for NOAA/NWS temperature data.

    Uses the api.weather.gov gridpoint endpoint for Modesto, CA.
    UPGRADE: Also fetches 'forecast' endpoint (Periods) for organic
    alignment with NOAA weather.gov website numbers.
    """

    # Modesto Gridpoint (Sacramento Weather Forecast Office)
    GRIDPOINT_URL = "https://api.weather.gov/gridpoints/STO/45,63"
    # The Source of Truth endpoint (matches website)
    FORECAST_URL = "https://api.weather.gov/gridpoints/STO/45,63/forecast"

    # Required User-Agent per NWS API policy
    HEADERS = {
        "User-Agent": "(duck-sun-modesto, github.com/user/duck-sun-modesto)",
        "Accept": "application/geo+json"
    }

    def __init__(self):
        logger.info("[NOAAProvider] Initializing provider...")
        self.last_fetch: Optional[datetime] = None
        self.cached_data: Optional[List[NOAATemperature]] = None
        self.cached_periods: Optional[List[NOAAPeriod]] = None

    def fetch(self) -> Optional[List[NOAATemperature]]:
        """
        Fetch temperature forecast from NWS.

        Returns:
            List of temperature records with time and temp_c,
            or None if the fetch fails.
        """
        logger.info("[NOAAProvider] Fetching data from api.weather.gov...")

        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(self.GRIDPOINT_URL, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[NOAAProvider] HTTP {resp.status_code}: {resp.text[:200]}")
                    return None

                data = resp.json()

            # Extract temperature values from the gridpoint data
            temps: List[NOAATemperature] = []
            temp_data = data.get('properties', {}).get('temperature', {}).get('values', [])

            if not temp_data:
                logger.warning("[NOAAProvider] No temperature data in response")
                return None

            for point in temp_data:
                valid_time = point.get('validTime', '')
                temp_c = point.get('value')

                if temp_c is None:
                    continue

                time_str = valid_time.split('/')[0] if '/' in valid_time else valid_time

                temps.append({
                    "time": time_str,
                    "temp_c": float(temp_c)
                })

            logger.info(f"[NOAAProvider] Retrieved {len(temps)} temperature records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except httpx.TimeoutException:
            logger.warning("[NOAAProvider] Request timed out")
            return None
        except httpx.RequestError as e:
            logger.warning(f"[NOAAProvider] Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"[NOAAProvider] Unexpected error: {e}", exc_info=True)
            return None

    async def fetch_async(self) -> Optional[List[NOAATemperature]]:
        """Fetch hourly temperature forecast (Numerical Grid)."""
        logger.info("[NOAAProvider] Async fetch from api.weather.gov (Gridpoints)...")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self.GRIDPOINT_URL, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[NOAAProvider] HTTP {resp.status_code}")
                    return None

                data = resp.json()

            temps: List[NOAATemperature] = []
            temp_data = data.get('properties', {}).get('temperature', {}).get('values', [])

            if not temp_data:
                logger.warning("[NOAAProvider] No temperature data in response")
                return None

            for point in temp_data:
                valid_time = point.get('validTime', '')
                temp_c = point.get('value')

                if temp_c is None:
                    continue

                time_str = valid_time.split('/')[0] if '/' in valid_time else valid_time

                temps.append({
                    "time": time_str,
                    "temp_c": float(temp_c)
                })

            logger.info(f"[NOAAProvider] Retrieved {len(temps)} hourly records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except Exception as e:
            logger.warning(f"[NOAAProvider] Async fetch failed: {e}")
            return None

    async def fetch_text_forecast(self) -> Optional[List[NOAATextForecast]]:
        """Fetch human-written text forecast for Narrative Override."""
        logger.info("[NOAAProvider] Fetching text forecast (Narrative)...")

        # Use cached periods if available
        if self.cached_periods:
            return [{"name": p['name'], "detailedForecast": p['detailedForecast']}
                    for p in self.cached_periods]

        # Otherwise fetch fresh
        periods = await self.fetch_forecast_periods()
        if periods:
            return [{"name": p['name'], "detailedForecast": p['detailedForecast']}
                    for p in periods]
        return None

    async def fetch_forecast_periods(self) -> Optional[List[NOAAPeriod]]:
        """
        Fetch the 'Period' forecast (Monday, Monday Night, etc.).
        This is the ORGANIC SOURCE OF TRUTH for the NWS website numbers.
        """
        logger.info("[NOAAProvider] Fetching text forecast periods (Website Match)...")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self.FORECAST_URL, headers=self.HEADERS)
                if resp.status_code != 200:
                    logger.warning(f"[NOAAProvider] Forecast API {resp.status_code}")
                    return None

                data = resp.json()
                periods = data.get('properties', {}).get('periods', [])

                self.cached_periods = periods
                logger.info(f"[NOAAProvider] Retrieved {len(periods)} forecast periods")
                return periods
        except Exception as e:
            logger.error(f"[NOAAProvider] Period fetch failed: {e}", exc_info=True)
            return None

    def get_daily_high_low(self) -> Dict[str, Dict[str, Any]]:
        """
        Process the Period data into Daily Highs/Lows.

        Returns:
            { '2025-12-14': {'high_f': 51, 'low_f': 41, 'condition': 'Cloudy'} }
        """
        if not self.cached_periods:
            return {}

        daily_map: Dict[str, Dict[str, Any]] = {}

        for p in self.cached_periods:
            is_day = p.get('isDaytime')
            temp = p.get('temperature')
            short_forecast = p.get('shortForecast', '')

            # Extract date from startTime (2025-12-14T18:00:00-08:00)
            start_time = p.get('startTime', '')
            if not start_time:
                continue

            date_str = start_time[:10]  # YYYY-MM-DD

            if date_str not in daily_map:
                daily_map[date_str] = {'high_f': None, 'low_f': None, 'condition': None}

            if is_day:
                daily_map[date_str]['high_f'] = temp
                # Use daytime forecast for the condition label
                if not daily_map[date_str]['condition']:
                    daily_map[date_str]['condition'] = short_forecast
            else:
                daily_map[date_str]['low_f'] = temp

        logger.info(f"[NOAAProvider] Processed {len(daily_map)} days from forecast periods")
        return daily_map

    def process_daily_high_low(self, hourly_data: Optional[List[NOAATemperature]]) -> dict:
        """
        Aggregate hourly NWS data into Daily High/Low for verification.
        
        Args:
            hourly_data: List of NWSTemperature records from fetch_async()
        
        Returns:
            Dictionary keyed by date string (YYYY-MM-DD):
            { '2025-12-12': {'high': 15.0, 'low': 5.2} }
        """
        if not hourly_data:
            logger.debug("[NOAAProvider] No hourly data to aggregate")
            return {}

        daily_map = {}

        for record in hourly_data:
            try:
                time_str = record['time']
                dt_str = time_str.split('T')[0] if 'T' in time_str else time_str[:10]
                temp = record['temp_c']
                
                if dt_str not in daily_map:
                    daily_map[dt_str] = {'temps': []}
                
                daily_map[dt_str]['temps'].append(temp)
                
            except Exception as e:
                logger.debug(f"[NOAAProvider] Failed to parse record: {e}")
                continue

        # Calculate Min/Max for each day
        results = {}
        for date_key, data in daily_map.items():
            temps = data['temps']
            if temps:
                results[date_key] = {
                    'high': max(temps),
                    'low': min(temps)
                }
                logger.debug(f"[NOAAProvider] Daily {date_key}: "
                           f"High={results[date_key]['high']:.1f}°C, "
                           f"Low={results[date_key]['low']:.1f}°C")
        
        logger.info(f"[NOAAProvider] Aggregated {len(results)} days from hourly data")
        return results


if __name__ == "__main__":
    import asyncio
    import json

    logging.basicConfig(level=logging.INFO)

    async def test():
        provider = NOAAProvider()

        print("=== Testing NOAA Provider ===\n")
        
        # Test hourly data
        data = await provider.fetch_async()
        if data:
            print(f"\nNWS Temperature Data ({len(data)} records):")
            print("-" * 50)
            for record in data[:10]:
                print(f"  {record['time']}: {record['temp_c']:.1f}C")
            print("  ...")
        
        # Test text forecast
        print("\n=== Testing Text Forecast ===\n")
        text_data = await provider.fetch_text_forecast()
        if text_data:
            print(f"Text Forecast ({len(text_data)} periods):")
            print("-" * 50)
            for period in text_data[:4]:
                print(f"\n{period['name']}:")
                print(f"  {period['detailedForecast'][:150]}...")
        else:
            print("Failed to fetch text forecast")

    asyncio.run(test())
