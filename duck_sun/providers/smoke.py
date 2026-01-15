"""
Smoke Provider for Duck Sun Modesto

Fetches air quality data (PM2.5 and AQI) to detect wildfire smoke events.
Smoke scatters solar irradiance differently than clouds, often "flattening"
the peak solar curve rather than creating the jagged profile of clouds.

Source: Open-Meteo Air Quality API

The "Smoke Shade" Effect:
- PM2.5 > 25 ug/m3: Moderate haze, ~5% solar loss
- PM2.5 > 50 ug/m3: Unhealthy for sensitive groups, ~15% solar loss
- PM2.5 > 100 ug/m3: Unhealthy, ~30% solar loss
- PM2.5 > 200 ug/m3: Very unhealthy/hazardous, ~50% solar loss ("Orange Sky")
"""

import httpx
import logging
from typing import Dict, Any, Optional, List, TypedDict

logger = logging.getLogger(__name__)


class SmokeMetrics(TypedDict):
    """Typed dictionary for smoke/air quality metrics."""
    time: str
    pm2_5: float
    us_aqi: int


class SmokeProvider:
    """
    Provider for Air Quality data (Wildfire Smoke detection).
    
    Fetches PM2.5 concentrations and US AQI from Open-Meteo Air Quality API.
    This data is used by the UncannyEngine to apply "Smoke Guard" penalties
    to solar forecasts during wildfire events.
    """
    
    # Modesto, CA coordinates
    LAT = 37.6391
    LON = -120.9969
    
    # Open-Meteo Air Quality API endpoint
    URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

    def __init__(self):
        """Initialize the SmokeProvider."""
        self.data: Optional[Dict[str, Any]] = None
        logger.debug("[SmokeProvider] Initialized for Modesto, CA")

    async def fetch_async(self, days: int = 5) -> Optional[List[SmokeMetrics]]:
        """
        Fetch PM2.5 and AQI forecast from Open-Meteo Air Quality API.
        
        Args:
            days: Number of forecast days (1-5, API limit)
            
        Returns:
            List of SmokeMetrics dictionaries with hourly data, or None on error
        """
        logger.info(f"[SmokeProvider] Fetching air quality data for {days} days...")
        
        # Clamp days to API limits
        days = max(1, min(days, 5))
        
        params = {
            "latitude": self.LAT,
            "longitude": self.LON,
            "hourly": ["pm2_5", "us_aqi"],
            "timezone": "America/Los_Angeles",
            "forecast_days": days
        }
        
        logger.debug(f"[SmokeProvider] Request params: {params}")

        try:
            async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
                logger.debug(f"[SmokeProvider] Sending GET request to {self.URL}")
                resp = await client.get(self.URL, params=params)
                
                logger.debug(f"[SmokeProvider] Response status: {resp.status_code}")
                
                if resp.status_code != 200:
                    logger.warning(f"[SmokeProvider] HTTP {resp.status_code}: {resp.text[:200]}")
                    return None
                
                data = resp.json()
                logger.debug(f"[SmokeProvider] Response keys: {list(data.keys())}")
                
                hourly = data.get("hourly", {})
                
                times = hourly.get("time", [])
                pm2_5 = hourly.get("pm2_5", [])
                aqi = hourly.get("us_aqi", [])
                
                logger.debug(f"[SmokeProvider] Raw data: {len(times)} time slots, "
                           f"{len(pm2_5)} PM2.5 values, {len(aqi)} AQI values")
                
                results: List[SmokeMetrics] = []
                null_count = 0
                
                for i, t in enumerate(times):
                    if i < len(pm2_5) and i < len(aqi):
                        # Handle None values from API (future hours may be null)
                        p_val = pm2_5[i] if pm2_5[i] is not None else 0.0
                        a_val = aqi[i] if aqi[i] is not None else 0
                        
                        if pm2_5[i] is None or aqi[i] is None:
                            null_count += 1
                        
                        results.append({
                            "time": t,
                            "pm2_5": float(p_val),
                            "us_aqi": int(a_val)
                        })
                
                if null_count > 0:
                    logger.debug(f"[SmokeProvider] {null_count} hours had null values (defaulted to 0)")
                
                # Log smoke summary
                if results:
                    max_pm = max(r['pm2_5'] for r in results)
                    max_aqi = max(r['us_aqi'] for r in results)
                    logger.info(f"[SmokeProvider] Retrieved {len(results)} hours of smoke data")
                    logger.info(f"[SmokeProvider] Max PM2.5: {max_pm:.1f} ug/m3, Max AQI: {max_aqi}")
                    
                    # Provide smoke level interpretation
                    if max_pm > 200:
                        logger.warning(f"[SmokeProvider] HAZARDOUS smoke levels detected! (PM2.5 > 200)")
                    elif max_pm > 100:
                        logger.warning(f"[SmokeProvider] UNHEALTHY smoke levels detected (PM2.5 > 100)")
                    elif max_pm > 50:
                        logger.info(f"[SmokeProvider] Moderate smoke levels present (PM2.5 > 50)")
                    else:
                        logger.info(f"[SmokeProvider] Air quality is GOOD (PM2.5 <= 50)")
                
                return results

        except httpx.TimeoutException as e:
            logger.error(f"[SmokeProvider] Request timed out: {e}")
            return None
        except httpx.RequestError as e:
            logger.error(f"[SmokeProvider] Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"[SmokeProvider] Unexpected error: {e}", exc_info=True)
            return None
    
    def get_smoke_level_description(self, pm2_5: float) -> str:
        """
        Get human-readable description of smoke level based on PM2.5.
        
        Args:
            pm2_5: PM2.5 concentration in ug/m3
            
        Returns:
            Human-readable smoke level description
        """
        if pm2_5 <= 12:
            return "Good (Clean Air)"
        elif pm2_5 <= 35:
            return "Moderate (Slight Haze)"
        elif pm2_5 <= 55:
            return "Unhealthy for Sensitive Groups"
        elif pm2_5 <= 150:
            return "Unhealthy (Visible Smoke)"
        elif pm2_5 <= 250:
            return "Very Unhealthy (Heavy Smoke)"
        else:
            return "Hazardous (Orange Sky)"


if __name__ == "__main__":
    # Test the provider
    import asyncio
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        print("=== Testing SmokeProvider ===\n")
        
        provider = SmokeProvider()
        data = await provider.fetch_async(days=3)
        
        if data:
            print(f"\nRetrieved {len(data)} hourly records")
            print("\nFirst 12 hours:")
            print("-" * 50)
            print(f"{'Time':<20} {'PM2.5':>10} {'AQI':>6} {'Level'}")
            print("-" * 50)
            
            for record in data[:12]:
                level = provider.get_smoke_level_description(record['pm2_5'])
                print(f"{record['time']:<20} {record['pm2_5']:>10.1f} {record['us_aqi']:>6} {level}")
            
            # Summary
            max_pm = max(r['pm2_5'] for r in data)
            avg_pm = sum(r['pm2_5'] for r in data) / len(data)
            print(f"\nMax PM2.5: {max_pm:.1f} ug/m3")
            print(f"Avg PM2.5: {avg_pm:.1f} ug/m3")
        else:
            print("Failed to fetch data")
    
    asyncio.run(test())
