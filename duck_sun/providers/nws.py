"""
National Weather Service (NWS) Provider for Duck Sun Modesto

Fetches official US government temperature forecasts from api.weather.gov.
NWS forecasts include human-in-the-loop adjustments for local topography
like the Central Valley, making them particularly valuable for Modesto.

Gridpoint: STO/45,63 (Sacramento WFO, Modesto grid coordinates)
"""

import httpx
import logging
from datetime import datetime
from typing import List, Optional, TypedDict

logger = logging.getLogger(__name__)


class NWSTemperature(TypedDict):
    time: str  # ISO format
    temp_c: float


class NWSProvider:
    """
    Provider for National Weather Service temperature data.

    Uses the api.weather.gov gridpoint endpoint for Modesto, CA.
    This provides official US government forecasts with local adjustments.
    """

    # Modesto Gridpoint (Sacramento Weather Forecast Office)
    GRIDPOINT_URL = "https://api.weather.gov/gridpoints/STO/45,63"

    # Required User-Agent per NWS API policy
    HEADERS = {
        "User-Agent": "(duck-sun-modesto, github.com/user/duck-sun-modesto)",
        "Accept": "application/geo+json"
    }

    def __init__(self):
        self.last_fetch: Optional[datetime] = None
        self.cached_data: Optional[List[NWSTemperature]] = None

    def fetch(self) -> Optional[List[NWSTemperature]]:
        """
        Fetch temperature forecast from NWS.

        Returns:
            List of temperature records with time and temp_c,
            or None if the fetch fails.
        """
        logger.info("[NWSProvider] Fetching data from api.weather.gov...")

        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(self.GRIDPOINT_URL, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[NWSProvider] HTTP {resp.status_code}: {resp.text[:200]}")
                    return None

                data = resp.json()

            # Extract temperature values from the gridpoint data
            # NWS uses ISO 8601 duration format: "validTime": "2025-01-01T00:00:00+00:00/PT1H"
            temps: List[NWSTemperature] = []

            temp_data = data.get('properties', {}).get('temperature', {}).get('values', [])

            if not temp_data:
                logger.warning("[NWSProvider] No temperature data in response")
                return None

            for point in temp_data:
                # Parse the validTime which includes duration
                # Format: "2025-01-01T06:00:00+00:00/PT3H"
                valid_time = point.get('validTime', '')
                temp_c = point.get('value')

                if temp_c is None:
                    continue

                # Extract just the start time (before the duration separator)
                time_str = valid_time.split('/')[0] if '/' in valid_time else valid_time

                # NWS already provides temperatures in Celsius
                temps.append({
                    "time": time_str,
                    "temp_c": float(temp_c)
                })

            logger.info(f"[NWSProvider] Retrieved {len(temps)} temperature records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except httpx.TimeoutException:
            logger.warning("[NWSProvider] Request timed out")
            return None
        except httpx.RequestError as e:
            logger.warning(f"[NWSProvider] Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"[NWSProvider] Unexpected error: {e}", exc_info=True)
            return None

    async def fetch_async(self) -> Optional[List[NWSTemperature]]:
        """
        Async version of fetch for concurrent data gathering.
        """
        logger.info("[NWSProvider] Async fetch from api.weather.gov...")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self.GRIDPOINT_URL, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[NWSProvider] HTTP {resp.status_code}")
                    return None

                data = resp.json()

            temps: List[NWSTemperature] = []
            temp_data = data.get('properties', {}).get('temperature', {}).get('values', [])

            if not temp_data:
                logger.warning("[NWSProvider] No temperature data in response")
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

            logger.info(f"[NWSProvider] Retrieved {len(temps)} temperature records")

            self.last_fetch = datetime.now()
            self.cached_data = temps

            return temps

        except Exception as e:
            logger.warning(f"[NWSProvider] Async fetch failed: {e}")
            return None

    def process_daily_high_low(self, hourly_data: Optional[List[NWSTemperature]]) -> dict:
        """
        Aggregate hourly NWS data into Daily High/Low for verification.
        
        Args:
            hourly_data: List of NWSTemperature records from fetch_async()
        
        Returns:
            Dictionary keyed by date string (YYYY-MM-DD):
            { '2025-12-12': {'high': 15.0, 'low': 5.2} }
        """
        if not hourly_data:
            logger.debug("[NWSProvider] No hourly data to aggregate")
            return {}

        daily_map = {}

        for record in hourly_data:
            try:
                # Parse "2025-12-12T08:00:00+00:00" -> "2025-12-12"
                time_str = record['time']
                dt_str = time_str.split('T')[0] if 'T' in time_str else time_str[:10]
                temp = record['temp_c']
                
                if dt_str not in daily_map:
                    daily_map[dt_str] = {'temps': []}
                
                daily_map[dt_str]['temps'].append(temp)
                
            except Exception as e:
                logger.debug(f"[NWSProvider] Failed to parse record: {e}")
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
                logger.debug(f"[NWSProvider] Daily {date_key}: "
                           f"High={results[date_key]['high']:.1f}°C, "
                           f"Low={results[date_key]['low']:.1f}°C")
        
        logger.info(f"[NWSProvider] Aggregated {len(results)} days from hourly data")
        return results


if __name__ == "__main__":
    # Test the provider
    import json

    logging.basicConfig(level=logging.INFO)

    provider = NWSProvider()
    data = provider.fetch()

    if data:
        print(f"\nNWS Temperature Data ({len(data)} records):")
        print("-" * 50)
        for record in data[:10]:  # Show first 10
            print(f"  {record['time']}: {record['temp_c']:.1f}C")
        print("  ...")
    else:
        print("Failed to fetch NWS data")
