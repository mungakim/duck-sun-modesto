"""
Open-Meteo Weather Provider for Duck Sun Modesto

This module fetches weather data from Open-Meteo API and computes
deterministic solar factors for Modesto, CA.

The solar calculations are done in Python (not LLM) to ensure 100% accuracy.

Open-Meteo aggregates multiple models including:
- GFS (US Global Forecast System)
- ICON (German DWD model)
- GEM (Canadian model)
"""

import httpx
import logging
from datetime import datetime
from typing import TypedDict, List, Optional

# Get logger (configuration is done in scheduler.py)
logger = logging.getLogger(__name__)


# Type definitions for strict data handling
class HourlyData(TypedDict):
    time: str
    solar_factor: float
    is_duck_hour: bool  # HE9-16 (Hour Ending 9 to 16)
    cloud_cover: int
    radiation: float
    temperature_c: float  # For consensus model
    dewpoint_c: float     # For fog detection
    wind_speed_kmh: float  # For fog stagnation check


class DailyForecast(TypedDict):
    date: str
    day_name: str
    high_c: float
    low_c: float
    high_f: int
    low_f: int
    precip_prob: int
    weather_code: int
    condition: str  # "Clear", "Partly Cloudy", "Mostly Cloudy", etc.


class ForecastResult(TypedDict):
    generated_at: str
    location: str
    daily_summary: List[HourlyData]
    daily_forecast: List[DailyForecast]  # 8-day daily forecast


# Modesto, CA coordinates
MODESTO_LAT = 37.6391
MODESTO_LON = -120.9969

# Maximum expected Global Horizontal Irradiance (W/m²)
MAX_GHI = 900.0


# WMO Weather Code to human-readable conditions
# Reference: https://open-meteo.com/en/docs
WEATHER_CODES = {
    0: "Clear",
    1: "Mostly Clear",
    2: "Partly Cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Fog",
    51: "Light Drizzle",
    53: "Drizzle",
    55: "Heavy Drizzle",
    56: "Freezing Drizzle",
    57: "Freezing Drizzle",
    61: "Light Rain",
    63: "Rain",
    65: "Heavy Rain",
    66: "Freezing Rain",
    67: "Freezing Rain",
    71: "Light Snow",
    73: "Snow",
    75: "Heavy Snow",
    77: "Snow Grains",
    80: "Light Showers",
    81: "Showers",
    82: "Heavy Showers",
    85: "Snow Showers",
    86: "Snow Showers",
    95: "Thunderstorm",
    96: "Thunderstorm",
    99: "Thunderstorm",
}


def weather_code_to_condition(code: int) -> str:
    """Convert WMO weather code to human-readable condition."""
    return WEATHER_CODES.get(code, "Unknown")


async def fetch_open_meteo(days: int = 8) -> ForecastResult:
    """
    Fetch raw weather data and compute deterministic solar factors.
    
    Args:
        days: Number of forecast days (1-7)
        
    Returns:
        ForecastResult with pre-calculated solar metrics
    """
    logger.info(f"[fetch_open_meteo] Starting fetch for {days} days forecast")
    logger.info(f"[fetch_open_meteo] Location: Modesto, CA ({MODESTO_LAT}, {MODESTO_LON})")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": MODESTO_LAT,
        "longitude": MODESTO_LON,
        "hourly": ["temperature_2m", "dewpoint_2m", "cloud_cover",
                   "wind_speed_10m", "shortwave_radiation", "direct_normal_irradiance",
                   "precipitation_probability", "precipitation"],
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_probability_max",
                  "precipitation_sum", "weather_code"],
        "timezone": "America/Los_Angeles",
        "forecast_days": days,
    }
    
    logger.debug(f"[fetch_open_meteo] Request params: {params}")
    
    async with httpx.AsyncClient() as client:
        logger.info(f"[fetch_open_meteo] Making request to Open-Meteo API...")
        resp = await client.get(url, params=params, timeout=30.0)
        logger.info(f"[fetch_open_meteo] Response status: {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
    
    logger.info(f"[fetch_open_meteo] Received {len(data.get('hourly', {}).get('time', []))} hourly records")
    
    hourly = data["hourly"]
    processed_data: List[HourlyData] = []
    
    for i, t in enumerate(hourly["time"]):
        # Parse hour to determine Duck Curve window (HE9-16 means 09:00 to 16:00)
        dt = datetime.fromisoformat(t)
        is_duck = 9 <= dt.hour <= 16

        sw = hourly["shortwave_radiation"][i]
        clouds = hourly["cloud_cover"][i]
        temp = hourly["temperature_2m"][i]
        dewpoint = hourly["dewpoint_2m"][i]
        wind = hourly["wind_speed_10m"][i]

        # Handle None values from API
        sw = sw if sw is not None else 0.0
        clouds = clouds if clouds is not None else 0
        temp = temp if temp is not None else 0.0
        dewpoint = dewpoint if dewpoint is not None else 0.0
        wind = wind if wind is not None else 0.0

        # 0-1 Normalization Logic
        # 900 W/m² is approximately max GHI for the region
        base_rad = min(sw / MAX_GHI, 1.0)
        cloud_penalty = clouds / 100.0

        # Formula: High radiation is good, clouds punish it significantly
        # Cloud penalty factor of 0.7 means heavy clouds reduce output by 70%
        factor = max(base_rad * (1.0 - (0.7 * cloud_penalty)), 0.0)

        hourly_data: HourlyData = {
            "time": t,
            "solar_factor": round(factor, 3),
            "is_duck_hour": is_duck,
            "cloud_cover": clouds,
            "radiation": sw,
            "temperature_c": round(temp, 1),
            "dewpoint_c": round(dewpoint, 1),
            "wind_speed_kmh": round(wind, 1)
        }
        processed_data.append(hourly_data)

        if is_duck and i < 20:  # Log first day's duck hours
            logger.debug(f"[fetch_open_meteo] Duck hour {t}: factor={factor:.3f}, clouds={clouds}%, rad={sw}W/m²")
    
    # Process daily forecast data
    daily_data = data.get("daily", {})
    daily_forecasts: List[DailyForecast] = []
    
    if daily_data and "time" in daily_data:
        for i, date_str in enumerate(daily_data["time"]):
            dt = datetime.fromisoformat(date_str)
            high_c = daily_data.get("temperature_2m_max", [None] * len(daily_data["time"]))[i]
            low_c = daily_data.get("temperature_2m_min", [None] * len(daily_data["time"]))[i]
            precip_prob = daily_data.get("precipitation_probability_max", [0] * len(daily_data["time"]))[i]
            weather_code = daily_data.get("weather_code", [0] * len(daily_data["time"]))[i]
            
            # Handle None values
            high_c = high_c if high_c is not None else 0.0
            low_c = low_c if low_c is not None else 0.0
            precip_prob = precip_prob if precip_prob is not None else 0
            weather_code = weather_code if weather_code is not None else 0
            
            daily_forecast: DailyForecast = {
                "date": date_str,
                "day_name": dt.strftime("%a"),  # Mon, Tue, Wed, etc.
                "high_c": round(high_c, 1),
                "low_c": round(low_c, 1),
                "high_f": round(high_c * 9/5 + 32),
                "low_f": round(low_c * 9/5 + 32),
                "precip_prob": int(precip_prob),
                "weather_code": weather_code,
                "condition": weather_code_to_condition(weather_code)
            }
            daily_forecasts.append(daily_forecast)
        
        logger.info(f"[fetch_open_meteo] Processed {len(daily_forecasts)} daily forecast records")
    
    result: ForecastResult = {
        "generated_at": datetime.now().isoformat(),
        "location": "Modesto, CA",
        "daily_summary": processed_data,
        "daily_forecast": daily_forecasts
    }
    
    # Log summary stats
    duck_hours = [h for h in processed_data if h["is_duck_hour"]]
    if duck_hours:
        avg_factor = sum(h["solar_factor"] for h in duck_hours) / len(duck_hours)
        logger.info(f"[fetch_open_meteo] Total duck hours: {len(duck_hours)}")
        logger.info(f"[fetch_open_meteo] Average duck hour solar factor: {avg_factor:.3f}")
    
    logger.info(f"[fetch_open_meteo] Completed processing {len(processed_data)} hourly records")
    
    return result


# Synchronous wrapper for testing
def fetch_open_meteo_sync(days: int = 4) -> ForecastResult:
    """Synchronous version of fetch_open_meteo for testing purposes."""
    import asyncio
    return asyncio.run(fetch_open_meteo(days))


if __name__ == "__main__":
    # Test the provider directly
    import asyncio
    import json
    
    async def test():
        logger.info("=== Testing Open-Meteo Provider ===")
        result = await fetch_open_meteo(days=2)
        print(json.dumps(result, indent=2))
        
    asyncio.run(test())

