"""
Open-Meteo Weather Provider for Duck Sun Modesto

This module fetches weather data from Open-Meteo API and computes
deterministic solar factors for Modesto, CA.

The solar calculations are done in Python (not LLM) to ensure 100% accuracy.

Open-Meteo aggregates multiple models including:
- GFS (US Global Forecast System)
- ICON (German DWD model)
- GEM (Canadian model)
- HRRR (High-Resolution Rapid Refresh) - 15-min updates, 3km resolution
"""

import httpx
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TypedDict, List, Optional, Dict, Any

# SSL: Use OS certificate store for PyInstaller exe compatibility
try:
    from duck_sun.ssl_helper import get_httpx_ssl_context
except ImportError:
    import ssl as _ssl
    def get_httpx_ssl_context():
        return _ssl.create_default_context()

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

# HRRR Cache configuration
HRRR_CACHE_DIR = Path("outputs")
HRRR_CACHE_FILE = HRRR_CACHE_DIR / "hrrr_cache.json"
HRRR_CACHE_TTL_MINUTES = 60  # HRRR updates every 15 min, cache for 1 hour


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
    
    async with httpx.AsyncClient(verify=get_httpx_ssl_context()) as client:
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


# =============================================================================
# HRRR (High-Resolution Rapid Refresh) Model Support
# =============================================================================
# HRRR provides:
# - 15-minute update cycle (most frequent of any model)
# - 3km horizontal resolution (best for local fog/stratus)
# - 48-hour forecast horizon
# - Excellent for fog vs. sun timing in Central Valley
# =============================================================================

class HRRRHourlyData(TypedDict):
    """Hourly data from HRRR model."""
    time: str
    temperature_c: float
    precipitation_prob: int
    precipitation_mm: float
    cloud_cover: int
    visibility_m: float
    shortwave_radiation: float
    is_fog: bool  # visibility < 1000m


class HRRRForecast(TypedDict):
    """48-hour HRRR forecast result."""
    generated_at: str
    model: str
    location: str
    hourly: List[HRRRHourlyData]
    daily_precip_prob: Dict[str, int]  # date -> max precip prob


def _load_hrrr_cache() -> Optional[dict]:
    """Load cached HRRR data if within TTL."""
    if not HRRR_CACHE_FILE.exists():
        return None

    try:
        with open(HRRR_CACHE_FILE, 'r', encoding='utf-8') as f:
            cache = json.load(f)

        cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
        age_minutes = (datetime.now() - cached_time).total_seconds() / 60

        logger.info(f"[HRRR] Cache age: {age_minutes:.1f} minutes")

        if age_minutes <= HRRR_CACHE_TTL_MINUTES:
            logger.info(f"[HRRR] Cache VALID (TTL: {HRRR_CACHE_TTL_MINUTES}m)")
            return cache
        else:
            logger.info("[HRRR] Cache EXPIRED")
            return None

    except Exception as e:
        logger.warning(f"[HRRR] Cache load error: {e}")
        return None


def _save_hrrr_cache(data: HRRRForecast) -> bool:
    """Save HRRR data to cache."""
    try:
        HRRR_CACHE_DIR.mkdir(exist_ok=True)
        cache = {
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        with open(HRRR_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
        logger.info(f"[HRRR] Cache saved: {len(data.get('hourly', []))} hours")
        return True
    except Exception as e:
        logger.error(f"[HRRR] Cache save failed: {e}")
        return False


async def fetch_hrrr_forecast(force_refresh: bool = False) -> Optional[HRRRForecast]:
    """
    Fetch HRRR (High-Resolution Rapid Refresh) forecast from Open-Meteo.

    HRRR is a NOAA model optimized for:
    - 15-minute update cycle (vs 6-hour for GFS)
    - 3km resolution (vs 13km for GFS)
    - 48-hour forecast window
    - Excellent fog/visibility prediction for Central Valley

    Returns:
        HRRRForecast with hourly data and daily precipitation probabilities
    """
    # Check cache first
    if not force_refresh:
        cache = _load_hrrr_cache()
        if cache and cache.get('data'):
            logger.info("[HRRR] CACHE HIT - Returning cached data")
            return cache['data']

    logger.info("[HRRR] Fetching fresh HRRR data from Open-Meteo...")

    # Open-Meteo HRRR endpoint
    # Docs: https://open-meteo.com/en/docs/gfs-api (HRRR is part of US models)
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": MODESTO_LAT,
        "longitude": MODESTO_LON,
        "hourly": [
            "temperature_2m",
            "precipitation_probability",
            "precipitation",
            "cloud_cover",
            "visibility",
            "shortwave_radiation"
        ],
        "models": "ncep_hrrr_conus",  # HRRR CONUS model (3km resolution, US only)
        "timezone": "America/Los_Angeles",
        "forecast_days": 2,  # HRRR is 48-hour max
    }

    try:
        async with httpx.AsyncClient(verify=get_httpx_ssl_context()) as client:
            logger.info(f"[HRRR] Making request to Open-Meteo (model=hrrr)...")
            resp = await client.get(url, params=params, timeout=30.0)
            logger.info(f"[HRRR] Response status: {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        logger.info(f"[HRRR] Received {len(times)} hourly records")

        hourly_data: List[HRRRHourlyData] = []
        daily_precip: Dict[str, int] = {}

        for i, t in enumerate(times):
            temp = hourly.get("temperature_2m", [None] * len(times))[i]
            precip_prob = hourly.get("precipitation_probability", [0] * len(times))[i]
            precip_mm = hourly.get("precipitation", [0] * len(times))[i]
            cloud = hourly.get("cloud_cover", [0] * len(times))[i]
            visibility = hourly.get("visibility", [10000] * len(times))[i]
            radiation = hourly.get("shortwave_radiation", [0] * len(times))[i]

            # Handle None values
            temp = temp if temp is not None else 0.0
            precip_prob = precip_prob if precip_prob is not None else 0
            precip_mm = precip_mm if precip_mm is not None else 0.0
            cloud = cloud if cloud is not None else 0
            visibility = visibility if visibility is not None else 10000
            radiation = radiation if radiation is not None else 0.0

            # Fog detection: visibility < 1000m (1km)
            is_fog = visibility < 1000

            hourly_data.append({
                "time": t,
                "temperature_c": round(temp, 1),
                "precipitation_prob": int(precip_prob),
                "precipitation_mm": round(precip_mm, 2),
                "cloud_cover": int(cloud),
                "visibility_m": round(visibility, 0),
                "shortwave_radiation": round(radiation, 1),
                "is_fog": is_fog
            })

            # Track daily max precip probability
            date_str = t[:10]  # YYYY-MM-DD
            if date_str not in daily_precip:
                daily_precip[date_str] = precip_prob
            else:
                daily_precip[date_str] = max(daily_precip[date_str], precip_prob)

        # Count fog hours
        fog_hours = sum(1 for h in hourly_data if h['is_fog'])
        logger.info(f"[HRRR] Fog hours detected: {fog_hours}")
        logger.info(f"[HRRR] Daily precip probs: {daily_precip}")

        result: HRRRForecast = {
            "generated_at": datetime.now().isoformat(),
            "model": "HRRR",
            "location": "Modesto, CA",
            "hourly": hourly_data,
            "daily_precip_prob": daily_precip
        }

        _save_hrrr_cache(result)
        return result

    except httpx.HTTPStatusError as e:
        logger.error(f"[HRRR] HTTP error: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"[HRRR] Fetch failed: {e}")
        return None


def get_precipitation_probabilities(
    om_data: Dict[str, Any],
    hrrr_data: Optional[HRRRForecast],
    weathercom_data: Optional[List[Dict]],
    accu_data: Optional[List[Dict]]
) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate precipitation probabilities from all sources.

    Returns:
        Dict[date_str, {
            'hrrr': int or None,
            'om': int or None,
            'weathercom': int or None,
            'accu': int or None,
            'consensus': int  # weighted average
        }]
    """
    precip_by_date: Dict[str, Dict[str, Any]] = {}

    # Open-Meteo daily precip
    for day in om_data.get('daily_forecast', []):
        date_str = day.get('date')
        if date_str:
            if date_str not in precip_by_date:
                precip_by_date[date_str] = {}
            precip_by_date[date_str]['om'] = day.get('precip_prob')

    # HRRR daily precip (highest priority for short-term)
    if hrrr_data:
        for date_str, prob in hrrr_data.get('daily_precip_prob', {}).items():
            if date_str not in precip_by_date:
                precip_by_date[date_str] = {}
            precip_by_date[date_str]['hrrr'] = prob

    # Weather.com precip
    if weathercom_data:
        for day in weathercom_data:
            date_str = day.get('date')
            if date_str:
                if date_str not in precip_by_date:
                    precip_by_date[date_str] = {}
                precip_by_date[date_str]['weathercom'] = day.get('precip_prob')

    # AccuWeather precip
    if accu_data:
        for day in accu_data:
            date_str = day.get('date')
            if date_str:
                if date_str not in precip_by_date:
                    precip_by_date[date_str] = {}
                precip_by_date[date_str]['accu'] = day.get('precip_prob')

    # Calculate consensus (weighted average)
    # Weights: HRRR=3 (best short-term), Weather.com=2, Accu=2, OM=1
    weights = {'hrrr': 3, 'weathercom': 2, 'accu': 2, 'om': 1}

    for date_str, probs in precip_by_date.items():
        total_val, total_weight = 0.0, 0.0
        for source, weight in weights.items():
            val = probs.get(source)
            if val is not None:
                total_val += val * weight
                total_weight += weight

        if total_weight > 0:
            probs['consensus'] = round(total_val / total_weight)
        else:
            probs['consensus'] = 0

    return precip_by_date


if __name__ == "__main__":
    # Test the provider directly
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def test():
        print("=" * 60)
        print("  OPEN-METEO PROVIDER TEST (with HRRR)")
        print("=" * 60)

        print("\n[1/2] Testing standard Open-Meteo forecast...")
        result = await fetch_open_meteo(days=2)
        print(f"  Received {len(result.get('daily_summary', []))} hourly records")
        print(f"  Daily forecast: {len(result.get('daily_forecast', []))} days")

        print("\n[2/2] Testing HRRR model forecast...")
        hrrr = await fetch_hrrr_forecast(force_refresh=True)
        if hrrr:
            print(f"  Model: {hrrr['model']}")
            print(f"  Hourly records: {len(hrrr['hourly'])}")
            print(f"  Daily precip probs: {hrrr['daily_precip_prob']}")
            fog_count = sum(1 for h in hrrr['hourly'] if h['is_fog'])
            print(f"  Fog hours detected: {fog_count}")
        else:
            print("  [FAILED] Could not fetch HRRR data")

        print("\n" + "=" * 60)

    asyncio.run(test())

