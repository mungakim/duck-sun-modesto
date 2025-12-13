"""
Met.no (Norwegian Meteorological Institute) Provider for Duck Sun Modesto

Fetches weather data from api.met.no which uses the ECMWF (European) model.
Met.no often outperforms US models in stability and runs one of the world's
most sophisticated ECMWF implementations.

This provider gives us a European model perspective to triangulate with
US models (GFS via Open-Meteo and NWS forecasts).
"""

import httpx
import logging
from datetime import datetime
from typing import List, Optional, TypedDict

logger = logging.getLogger(__name__)


class MetNoTemperature(TypedDict):
    time: str  # ISO format
    temp_c: float


class MetNoProvider:
    """
    Provider for Met.no (YR.no backend) temperature data.

    Uses the Locationforecast 2.0 API with compact format.
    This provides ECMWF-based forecasts from the Norwegian Met Institute.
    """

    BASE_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"

    # Modesto, CA coordinates
    LAT = 37.6391
    LON = -120.9969

    # Required User-Agent per Met.no API terms
    HEADERS = {
        "User-Agent": "DuckSunModesto/1.0 github.com/user/duck-sun-modesto"
    }

    def __init__(self):
        self.last_fetch: Optional[datetime] = None
        self.cached_data: Optional[List[MetNoTemperature]] = None

    def fetch(self) -> Optional[List[MetNoTemperature]]:
        """
        Fetch temperature forecast from Met.no.

        Returns:
            List of temperature records with time and temp_c,
            or None if the fetch fails.
        """
        logger.info("[MetNoProvider] Fetching data from api.met.no...")

        params = {
            "lat": self.LAT,
            "lon": self.LON
        }

        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(self.BASE_URL, params=params, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[MetNoProvider] HTTP {resp.status_code}: {resp.text[:200]}")
                    return None

                data = resp.json()

            # Extract temperature from timeseries data
            timeseries = data.get('properties', {}).get('timeseries', [])

            if not timeseries:
                logger.warning("[MetNoProvider] No timeseries data in response")
                return None

            temps: List[MetNoTemperature] = []

            for item in timeseries:
                time_str = item.get('time', '')
                instant_data = item.get('data', {}).get('instant', {}).get('details', {})
                temp_c = instant_data.get('air_temperature')

                if temp_c is None:
                    continue

                temps.append({
                    "time": time_str,
                    "temp_c": float(temp_c)
                })

            logger.info(f"[MetNoProvider] Retrieved {len(temps)} temperature records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except httpx.TimeoutException:
            logger.warning("[MetNoProvider] Request timed out")
            return None
        except httpx.RequestError as e:
            logger.warning(f"[MetNoProvider] Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"[MetNoProvider] Unexpected error: {e}", exc_info=True)
            return None

    async def fetch_async(self) -> Optional[List[MetNoTemperature]]:
        """
        Async version of fetch for concurrent data gathering.
        """
        logger.info("[MetNoProvider] Async fetch from api.met.no...")

        params = {
            "lat": self.LAT,
            "lon": self.LON
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self.BASE_URL, params=params, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[MetNoProvider] HTTP {resp.status_code}")
                    return None

                data = resp.json()

            timeseries = data.get('properties', {}).get('timeseries', [])

            if not timeseries:
                logger.warning("[MetNoProvider] No timeseries data in response")
                return None

            temps: List[MetNoTemperature] = []

            for item in timeseries:
                time_str = item.get('time', '')
                instant_data = item.get('data', {}).get('instant', {}).get('details', {})
                temp_c = instant_data.get('air_temperature')

                if temp_c is None:
                    continue

                temps.append({
                    "time": time_str,
                    "temp_c": float(temp_c)
                })

            logger.info(f"[MetNoProvider] Retrieved {len(temps)} temperature records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except Exception as e:
            logger.warning(f"[MetNoProvider] Async fetch failed: {e}")
            return None

    def process_daily_high_low(self, hourly_data: Optional[List[MetNoTemperature]]) -> dict:
        """
        Aggregate hourly Met.no data into Daily High/Low for verification.
        
        Args:
            hourly_data: List of MetNoTemperature records from fetch_async()
        
        Returns:
            Dictionary keyed by date string (YYYY-MM-DD):
            { '2025-12-12': {'high': 15.0, 'low': 5.2} }
        """
        if not hourly_data:
            logger.debug("[MetNoProvider] No hourly data to aggregate")
            return {}

        daily_map = {}

        for record in hourly_data:
            try:
                # Met.no uses ISO format like 2025-12-12T12:00:00Z
                time_str = record['time']
                dt_str = time_str.split('T')[0] if 'T' in time_str else time_str[:10]
                temp = record['temp_c']
                
                if dt_str not in daily_map:
                    daily_map[dt_str] = {'temps': []}
                
                daily_map[dt_str]['temps'].append(temp)
                
            except Exception as e:
                logger.debug(f"[MetNoProvider] Failed to parse record: {e}")
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
                logger.debug(f"[MetNoProvider] Daily {date_key}: "
                           f"High={results[date_key]['high']:.1f}°C, "
                           f"Low={results[date_key]['low']:.1f}°C")
        
        logger.info(f"[MetNoProvider] Aggregated {len(results)} days from hourly data")
        return results


if __name__ == "__main__":
    # Test the provider
    import json

    logging.basicConfig(level=logging.INFO)

    provider = MetNoProvider()
    data = provider.fetch()

    if data:
        print(f"\nMet.no Temperature Data ({len(data)} records):")
        print("-" * 50)
        for record in data[:10]:  # Show first 10
            print(f"  {record['time']}: {record['temp_c']:.1f}C")
        print("  ...")
    else:
        print("Failed to fetch Met.no data")
