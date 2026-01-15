"""
Duck Sun Modesto Scheduler - Full Provider Edition

Orchestrates the daily solar forecasting workflow:
1. Fetch weather data from ALL 9 providers with retry + fallback
2. Run physics engine for solar/fog analysis
3. Generate the PDF Report (The Gold Bar) for Power System Schedulers

Features:
- Resilient fetching with 2 retries per provider
- Last Known Good (LKG) cache ensures PDF never shows "--"
- Lessons learned tracking for reliability analysis
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Core providers
from duck_sun.providers.open_meteo import fetch_open_meteo, fetch_hrrr_forecast
from duck_sun.providers.noaa import NOAAProvider
from duck_sun.providers.met_no import MetNoProvider
from duck_sun.providers.accuweather import AccuWeatherProvider
from duck_sun.providers.google_weather import GoogleWeatherProvider
from duck_sun.providers.mid_org import MIDOrgProvider
from duck_sun.providers.metar import MetarProvider
from duck_sun.providers.smoke import SmokeProvider
from duck_sun.providers.weather_com import WeatherComProvider
from duck_sun.providers.wunderground import WUndergroundProvider

# Processing
from duck_sun.uncanniness import UncannyEngine
from duck_sun.pdf_report import generate_pdf_report

# Resilience infrastructure
from duck_sun.resilience import with_retry, RetryConfig, categorize_error
from duck_sun.cache_manager import CacheManager, FetchResult

# Verification system (Truth Tracker)
from duck_sun.verification import TruthTracker, run_daily_verification

# Load environment variables
load_dotenv()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/duck_sun.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("outputs")
REPORT_DIR = Path("reports")

# Conservative retry config: 2 retries, 1-5s delays
RETRY_CONFIG = RetryConfig(
    max_retries=2,
    base_delay_seconds=1.0,
    max_delay_seconds=5.0,
    jitter=True
)

# Report-level retry configuration
MAX_REPORT_RETRIES = 3        # Total validation attempts (initial + 2 retries)
RETRY_DELAY_SECONDS = 23      # Wait between retries

# Minimum expected days per provider
EXPECTED_DAYS = {
    "accuweather": 5,      # $2/mo tier
    "google_weather": 4,   # 96 hours
    "noaa": 5,             # Usually 7, but 5 minimum acceptable
    "open_meteo": 8,       # Baseline - always needed
    "met_no": 6,           # Usually 8+
}


@dataclass
class ValidationResult:
    """Result of data completeness validation."""
    is_acceptable: bool
    critical_failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    provider_day_counts: Dict[str, int] = field(default_factory=dict)


def ensure_directories():
    """Create output directories if they don't exist."""
    logger.info("[ensure_directories] Ensuring output directories exist")
    OUTPUT_DIR.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "cache").mkdir(exist_ok=True)
    logger.info(f"[ensure_directories] OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.info(f"[ensure_directories] REPORT_DIR: {REPORT_DIR.absolute()}")


def verify_data_completeness(results: Dict[str, 'FetchResult']) -> ValidationResult:
    """
    Verify all critical providers returned expected data.

    Checks that high-weight providers (AccuWeather, Google, NOAA, Open-Meteo)
    have returned the expected number of forecast days.

    Returns:
        ValidationResult with pass/fail status and details
    """
    critical_failures = []
    warnings = []
    day_counts = {}

    # AccuWeather: Expect 5 days ($2/mo tier)
    accu = results.get("accuweather")
    if accu and accu.data:
        accu_days = len(accu.data) if isinstance(accu.data, list) else 0
        day_counts["AccuWeather"] = accu_days
        if accu_days < EXPECTED_DAYS["accuweather"]:
            critical_failures.append(f"AccuWeather: {accu_days}/{EXPECTED_DAYS['accuweather']} days")
    else:
        critical_failures.append("AccuWeather: No data")
        day_counts["AccuWeather"] = 0

    # Google Weather: Expect 4+ days (96 hours)
    google = results.get("google_weather")
    if google and google.data:
        google_daily = google.data.get("daily", []) if isinstance(google.data, dict) else []
        day_counts["Google"] = len(google_daily)
        if len(google_daily) < EXPECTED_DAYS["google_weather"]:
            critical_failures.append(f"Google: {len(google_daily)}/{EXPECTED_DAYS['google_weather']} days")
    else:
        critical_failures.append("Google: No data")
        day_counts["Google"] = 0

    # NOAA: Count unique days from hourly data
    noaa = results.get("noaa")
    if noaa and noaa.data:
        noaa_days = _count_unique_days_noaa(noaa.data)
        day_counts["NOAA"] = noaa_days
        if noaa_days < EXPECTED_DAYS["noaa"]:
            warnings.append(f"NOAA: {noaa_days}/{EXPECTED_DAYS['noaa']} days")
    else:
        warnings.append("NOAA: No data")
        day_counts["NOAA"] = 0

    # Open-Meteo: Expect 8 days (baseline - always needed)
    om = results.get("open_meteo")
    if om and om.data:
        om_daily = om.data.get("daily_forecast", []) if isinstance(om.data, dict) else []
        day_counts["Open-Meteo"] = len(om_daily)
        if len(om_daily) < EXPECTED_DAYS["open_meteo"]:
            critical_failures.append(f"Open-Meteo: {len(om_daily)}/{EXPECTED_DAYS['open_meteo']} days")
    else:
        critical_failures.append("Open-Meteo: No data")
        day_counts["Open-Meteo"] = 0

    # Met.no: Non-critical but tracked
    met = results.get("met_no")
    if met and met.data:
        met_days = _count_unique_days_met(met.data)
        day_counts["Met.no"] = met_days
        if met_days < EXPECTED_DAYS["met_no"]:
            warnings.append(f"Met.no: {met_days}/{EXPECTED_DAYS['met_no']} days")
    else:
        warnings.append("Met.no: No data")
        day_counts["Met.no"] = 0

    is_acceptable = len(critical_failures) == 0

    return ValidationResult(
        is_acceptable=is_acceptable,
        critical_failures=critical_failures,
        warnings=warnings,
        provider_day_counts=day_counts
    )


def _count_unique_days_noaa(data: List[Dict]) -> int:
    """Count unique days in NOAA hourly data."""
    if not data or not isinstance(data, list):
        return 0
    dates = set()
    for record in data:
        if isinstance(record, dict):
            # NOAA uses 'valid_time' or 'time' key
            time_str = record.get('valid_time', record.get('time', ''))
            if time_str:
                dates.add(time_str[:10])  # Extract YYYY-MM-DD
    return len(dates)


def _count_unique_days_met(data: List[Dict]) -> int:
    """Count unique days in Met.no data."""
    if not data or not isinstance(data, list):
        return 0
    dates = set()
    for record in data:
        if isinstance(record, dict):
            time_str = record.get('time', '')
            if time_str:
                dates.add(time_str[:10])  # Extract YYYY-MM-DD
    return len(dates)


def get_failed_provider_names(validation: ValidationResult) -> List[str]:
    """
    Extract provider names from validation failures for retry.

    Maps display names (AccuWeather) back to internal names (accuweather).
    """
    name_map = {
        "AccuWeather": "accuweather",
        "Google": "google_weather",
        "NOAA": "noaa",
        "Open-Meteo": "open_meteo",
        "Met.no": "met_no",
    }

    failed = []
    for failure in validation.critical_failures:
        # Extract provider name from failure string like "AccuWeather: 1/5 days"
        provider_display = failure.split(":")[0].strip()
        provider_internal = name_map.get(provider_display)
        if provider_internal and provider_internal not in failed:
            failed.append(provider_internal)

    return failed


async def fetch_with_retry(
    provider_name: str,
    fetch_func,
    cache_mgr: CacheManager,
    *args,
    **kwargs
) -> FetchResult:
    """
    Fetch data from a provider with retry logic and cache fallback.

    Args:
        provider_name: Name for logging/caching
        fetch_func: Async function to call
        cache_mgr: CacheManager instance
        *args, **kwargs: Arguments to pass to fetch_func

    Returns:
        FetchResult with data guaranteed (fresh, cached, or default)
    """
    start = time.time()
    fresh_data = None
    error_msg = None

    # Apply retry decorator dynamically
    @with_retry(config=RETRY_CONFIG, provider_name=provider_name)
    async def _fetch():
        return await fetch_func(*args, **kwargs)

    try:
        fresh_data = await _fetch()
    except Exception as e:
        error_type, error_msg = categorize_error(e)
        logger.error(f"[fetch_with_retry] {provider_name} failed: {error_msg}")

    elapsed = time.time() - start

    # Use cache manager for fallback
    result = cache_mgr.get_with_fallback(provider_name, fresh_data, error_msg)

    if result.source == "API":
        logger.info(f"[fetch_with_retry] {provider_name}: FRESH ({elapsed:.2f}s)")
    elif result.source == "CACHE":
        logger.info(f"[fetch_with_retry] {provider_name}: {result.status_label}")
    else:
        logger.warning(f"[fetch_with_retry] {provider_name}: DEFAULT (no cache)")

    return result


async def fetch_all_providers(cache_mgr: CacheManager) -> Dict[str, FetchResult]:
    """
    Fetch data from ALL 9 providers with retry + fallback.

    Returns:
        Dict mapping provider name to FetchResult
        Every FetchResult has data (never None)
    """
    results: Dict[str, FetchResult] = {}

    logger.info("[fetch_all_providers] Starting fetch from 11 providers...")

    # 1. Open-Meteo (primary source - required)
    logger.info("[fetch_all_providers] Fetching Open-Meteo...")
    results["open_meteo"] = await fetch_with_retry(
        "open_meteo",
        fetch_open_meteo,
        cache_mgr,
        days=8
    )

    # 2. HRRR (high-resolution model)
    logger.info("[fetch_all_providers] Fetching HRRR...")
    results["hrrr"] = await fetch_with_retry(
        "hrrr",
        fetch_hrrr_forecast,
        cache_mgr
    )

    # 3. NOAA (US government - weight 3x)
    logger.info("[fetch_all_providers] Fetching NOAA...")

    async def _fetch_noaa():
        noaa = NOAAProvider()
        return await noaa.fetch_async()

    results["noaa"] = await fetch_with_retry("noaa", _fetch_noaa, cache_mgr)

    # 4. Met.no (ECMWF model - weight 3x)
    logger.info("[fetch_all_providers] Fetching Met.no...")

    async def _fetch_met():
        met = MetNoProvider()
        return await met.fetch_async()

    results["met_no"] = await fetch_with_retry("met_no", _fetch_met, cache_mgr)

    # 5. AccuWeather (commercial - weight 4x)
    logger.info("[fetch_all_providers] Fetching AccuWeather...")

    async def _fetch_accu():
        accu = AccuWeatherProvider()
        return await accu.fetch_forecast()

    results["accuweather"] = await fetch_with_retry("accuweather", _fetch_accu, cache_mgr)

    # 6. Google Weather (MetNet-3 neural model - weight 6x)
    logger.info("[fetch_all_providers] Fetching Google Weather (MetNet-3)...")

    async def _fetch_google():
        google = GoogleWeatherProvider()
        return await google.fetch_forecast(hours=96)

    results["google_weather"] = await fetch_with_retry("google_weather", _fetch_google, cache_mgr)

    # 7. Weather.com (commercial - weight 4x)
    logger.info("[fetch_all_providers] Fetching Weather.com...")

    async def _fetch_weather_com():
        wcom = WeatherComProvider()
        return wcom.fetch_sync()  # curl_cffi is sync

    results["weather_com"] = await fetch_with_retry("weather_com", _fetch_weather_com, cache_mgr)

    # 8. Weather Underground (commercial - weight 4x)
    logger.info("[fetch_all_providers] Fetching Weather Underground...")

    async def _fetch_wunderground():
        wunder = WUndergroundProvider()
        return wunder.fetch_sync()  # curl_cffi is sync

    results["wunderground"] = await fetch_with_retry("wunderground", _fetch_wunderground, cache_mgr)

    # 9. MID.org (local ground truth - weight 2x)
    logger.info("[fetch_all_providers] Fetching MID.org...")

    async def _fetch_mid():
        mid = MIDOrgProvider()
        return await mid.fetch_48hr_summary()

    results["mid_org"] = await fetch_with_retry("mid_org", _fetch_mid, cache_mgr)

    # 8. METAR (airport observations)
    logger.info("[fetch_all_providers] Fetching METAR...")

    async def _fetch_metar():
        metar = MetarProvider()
        raw = await metar.fetch_async()
        return metar.parse_metar(raw) if raw else None

    results["metar"] = await fetch_with_retry("metar", _fetch_metar, cache_mgr)

    # 9. Smoke (air quality)
    logger.info("[fetch_all_providers] Fetching Smoke/AQI...")

    async def _fetch_smoke():
        smoke = SmokeProvider()
        return await smoke.fetch_async(days=3)

    results["smoke"] = await fetch_with_retry("smoke", _fetch_smoke, cache_mgr)

    # Summary
    fresh_count = sum(1 for r in results.values() if r.source == "API")
    cache_count = sum(1 for r in results.values() if r.source == "CACHE")
    default_count = sum(1 for r in results.values() if r.source == "DEFAULT")

    logger.info(
        f"[fetch_all_providers] Complete: {fresh_count} fresh, "
        f"{cache_count} cached, {default_count} default"
    )

    return results


async def retry_single_provider(
    provider_name: str,
    cache_mgr: CacheManager
) -> FetchResult:
    """
    Re-fetch a single provider that failed validation.

    Maps provider names to their fetch functions and re-attempts the fetch.

    Args:
        provider_name: Internal provider name (e.g., "accuweather")
        cache_mgr: CacheManager instance

    Returns:
        FetchResult from the retry attempt
    """
    logger.info(f"[retry_single_provider] Retrying {provider_name}...")

    # Map provider names to their fetch functions
    if provider_name == "accuweather":
        async def _fetch():
            provider = AccuWeatherProvider()
            return await provider.fetch_forecast()
        return await fetch_with_retry(provider_name, _fetch, cache_mgr)

    elif provider_name == "google_weather":
        async def _fetch():
            provider = GoogleWeatherProvider()
            return await provider.fetch_forecast(hours=96)
        return await fetch_with_retry(provider_name, _fetch, cache_mgr)

    elif provider_name == "noaa":
        async def _fetch():
            provider = NOAAProvider()
            return await provider.fetch_async()
        return await fetch_with_retry(provider_name, _fetch, cache_mgr)

    elif provider_name == "met_no":
        async def _fetch():
            provider = MetNoProvider()
            return await provider.fetch_async()
        return await fetch_with_retry(provider_name, _fetch, cache_mgr)

    elif provider_name == "open_meteo":
        return await fetch_with_retry(
            provider_name,
            fetch_open_meteo,
            cache_mgr,
            days=8
        )

    else:
        logger.warning(f"[retry_single_provider] Unknown provider: {provider_name}")
        # Return a default FetchResult
        return cache_mgr.get_with_fallback(provider_name, None, "Unknown provider")


async def main():
    """Main scheduler entry point - Full Provider Edition."""
    pacific = ZoneInfo("America/Los_Angeles")
    start_time = datetime.now(pacific)
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")

    logger.info("=" * 60)
    logger.info(f"Duck Sun Modesto (Full Provider) - Run: {timestamp}")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # Initialize cache manager
        cache_mgr = CacheManager()
        cache_mgr.increment_run_count()

        # --- STEP 0: AUTOMATED VERIFICATION (Truth Tracker) ---
        logger.info("")
        logger.info("STEP 0: Running Truth Tracker (Yesterday's Verification)...")
        logger.info("-" * 40)

        tracker = TruthTracker()
        try:
            verify_result = await run_daily_verification(tracker)
            if verify_result:
                leaderboard = verify_result.get('leaderboard', [])
                if leaderboard:
                    top_dog = leaderboard[0]['source']
                    top_mae = leaderboard[0]['combined_mae']
                    logger.info(f"LEADERBOARD UPDATE: {top_dog} is currently #1 (MAE: {top_mae}C)")

                    # Log top 3
                    for entry in leaderboard[:3]:
                        logger.info(f"  #{entry['rank']} {entry['source']}: "
                                  f"High MAE={entry['high_error_mae']}C, "
                                  f"Low MAE={entry['low_error_mae']}C")

                    # ADVANCED: Dynamic Weight Alert (if Google falls out of Top 3)
                    google_rank = next((x['rank'] for x in leaderboard if x['source'] == 'Google'), 99)
                    if google_rank > 3:
                        logger.error(f"GOOGLE WEATHER ACCURACY ALERT: Ranked #{google_rank}")
                else:
                    logger.info("No verification data yet (need 24+ hours of forecasts)")
            else:
                logger.info("Could not fetch yesterday's actuals - skipping verification")
        except Exception as e:
            logger.warning(f"Verification failed (non-critical): {e}")
        finally:
            tracker.close()

        # --- STEP 1: Fetch ALL Data Sources ---
        logger.info("")
        logger.info("STEP 1: Fetching weather data from ALL 9 providers...")
        logger.info("-" * 40)

        results = await fetch_all_providers(cache_mgr)

        # --- STEP 1b: Validate Data Completeness & Selective Retry ---
        for attempt in range(MAX_REPORT_RETRIES):
            validation = verify_data_completeness(results)

            # Log validation results
            logger.info(f"[main] Data validation (attempt {attempt + 1}/{MAX_REPORT_RETRIES}): {validation.provider_day_counts}")

            if validation.is_acceptable:
                logger.info("[main] All critical providers have complete data")
                break

            # Log failures
            for failure in validation.critical_failures:
                logger.warning(f"[main] INCOMPLETE: {failure}")
            for warning in validation.warnings:
                logger.info(f"[main] Warning: {warning}")

            # Get list of failed provider names
            failed_providers = get_failed_provider_names(validation)

            # Retry ONLY failed providers (if not last attempt)
            if attempt < MAX_REPORT_RETRIES - 1 and failed_providers:
                logger.info(f"[main] Retrying {len(failed_providers)} failed providers in {RETRY_DELAY_SECONDS}s...")
                await asyncio.sleep(RETRY_DELAY_SECONDS)

                # Invalidate cache and re-fetch ONLY failed providers
                for provider_name in failed_providers:
                    cache_mgr.invalidate_cache(provider_name)
                    new_result = await retry_single_provider(provider_name, cache_mgr)
                    results[provider_name] = new_result
                    data_count = len(new_result.data) if new_result.data and isinstance(new_result.data, (list, dict)) else 0
                    if isinstance(new_result.data, dict):
                        data_count = len(new_result.data.get("daily", new_result.data.get("daily_forecast", [])))
                    logger.info(f"[main] Re-fetched {provider_name}: {data_count} records")
            else:
                if attempt == MAX_REPORT_RETRIES - 1:
                    logger.warning("[main] Max retries reached - proceeding with best available data")

        # Extract data from results
        om_data = results["open_meteo"].data
        hrrr_data = results["hrrr"].data
        noaa_data = results["noaa"].data
        met_data = results["met_no"].data
        accu_data = results["accuweather"].data
        google_data = results["google_weather"].data
        weather_com_data = results["weather_com"].data
        wunderground_data = results["wunderground"].data
        mid_data = results["mid_org"].data
        metar_data = results["metar"].data
        smoke_data = results["smoke"].data

        # Check critical provider
        if om_data is None or not om_data:
            logger.error("CRITICAL: Open-Meteo data unavailable - cannot continue")
            return 1

        # --- SPECIAL HANDLING FOR NOAA PERIOD DATA ---
        # Fetch the Period-based forecast for website alignment
        noaa_daily_periods = {}
        try:
            noaa_periods_provider = NOAAProvider()
            await noaa_periods_provider.fetch_forecast_periods()
            noaa_daily_periods = noaa_periods_provider.get_daily_high_low()
            logger.info(f"[main] NOAA Period Daily Stats: {len(noaa_daily_periods)} days")
            for date_key, stats in list(noaa_daily_periods.items())[:3]:
                logger.info(f"[main]   {date_key}: Hi={stats.get('high_f')}F, Lo={stats.get('low_f')}F")
        except Exception as e:
            logger.warning(f"[main] NOAA period fetch failed: {e}")

        # --- STEP 2: Run Physics Engine ---
        logger.info("")
        logger.info("STEP 2: Running Uncanny Engine (Physics)...")
        logger.info("-" * 40)

        engine = UncannyEngine()
        logger.info("[main] Normalizing temperatures from all sources...")

        # Pass additional sources to normalize_temps if available
        df = engine.normalize_temps(
            om_data,
            noaa_data if noaa_data else None,
            met_data if met_data else None,
            accu_data=accu_data if accu_data else None,
            mid_data=mid_data if mid_data else None
        )

        # Extract Google hourly data for hybrid solar calculations
        google_hourly = None
        if google_data and isinstance(google_data, dict):
            google_hourly = google_data.get('hourly', [])
            google_daily_list = google_data.get('daily', [])
            logger.info(f"[main] Extracted {len(google_hourly) if google_hourly else 0} Google hourly records")
            logger.info(f"[main] Extracted {len(google_daily_list)} Google daily records")
            if google_daily_list:
                logger.info(f"[main] Google daily sample: {google_daily_list[0]}")
        else:
            logger.warning(f"[main] google_data is None or not a dict: {type(google_data)}")

        logger.info("[main] Analyzing duck curve and fog risk (Hybrid Solar Physics)...")
        df_analyzed = engine.analyze_duck_curve(df, google_hourly=google_hourly)

        # Count risk levels (including Tule Fog specific detection)
        critical_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
        tule_fog_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('TULE FOG', na=False)])
        moderate_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('MODERATE', na=False)])

        if tule_fog_hours > 0:
            logger.warning(f"[main] TULE FOG ALERT: {tule_fog_hours} hours with Central Valley radiation fog")
        if critical_hours > 0:
            logger.warning(f"[main] CRITICAL FOG ALERT: {critical_hours} critical hours detected")
        elif moderate_hours > 0:
            logger.info(f"[main] FOG RISK: {moderate_hours} hours under monitoring")
        else:
            logger.info("[main] No fog conditions detected")

        # Build active sources list
        active_sources = []
        for name, result in results.items():
            if result.source != "DEFAULT" and result.data:
                active_sources.append(name)

        # Save Raw Data JSON
        json_path = OUTPUT_DIR / f"solar_data_{timestamp}.json"
        logger.info(f"[main] Saving raw data to {json_path}")

        consensus_data = {
            "generated_at": om_data.get("generated_at", timestamp) if isinstance(om_data, dict) else timestamp,
            "location": "Modesto, CA",
            "sources": active_sources,
            "provider_count": len(active_sources),
            "8_day_outlook": engine.get_daily_summary(df_analyzed, days=8),
            "duck_curve_tomorrow": engine.get_duck_curve_hours(df_analyzed),
            "reliability": cache_mgr.get_lessons_learned()
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(consensus_data, f, indent=2, default=str)

        logger.info(f"[main] ✓ Raw data saved to: {json_path}")

        # --- STEP 3: Generate PDF ---
        logger.info("")
        logger.info("STEP 3: Generating PDF Report...")
        logger.info("-" * 40)

        # Get degraded providers for warning banner
        degraded = cache_mgr.get_degraded_providers(results)
        if degraded:
            logger.warning(f"[main] Degraded providers: {', '.join(degraded)}")

        # Build PRECIP consensus data with range-aware source selection
        # Google MetNet-3 is best for 0-72 hours (days 0-2)
        # AccuWeather is better for 72+ hours (days 3+) - physics models beat neural at longer ranges
        precip_data = {}
        today = start_time.strftime('%Y-%m-%d')

        # Step 1: Open-Meteo as fallback base (always has 8 days)
        if om_data and 'daily_forecast' in om_data:
            for d in om_data['daily_forecast']:
                date_key = d.get('date', '')
                if date_key:
                    precip_data[date_key] = {
                        'consensus': d.get('precip_prob', 0),
                        'source': 'Open-Meteo'
                    }

        # Step 2: AccuWeather overwrites Open-Meteo (better quality, 5 days)
        if accu_data:
            for d in accu_data:
                date_key = d.get('date', '')
                if date_key and d.get('precip_prob') is not None:
                    precip_data[date_key] = {
                        'consensus': d.get('precip_prob', 0),
                        'source': 'AccuWeather'
                    }

        # Step 3: Google Weather overwrites BUT ONLY for days 0-2 (0-72 hours)
        # MetNet-3 neural model is optimized for short-term "nowcasting"
        # At 72+ hours, physics models (AccuWeather) are more reliable for precip
        if google_data and 'daily' in google_data:
            from datetime import datetime as dt_class
            today_dt = dt_class.strptime(today, '%Y-%m-%d')

            for d in google_data['daily']:
                date_key = d.get('date', '')
                if date_key and d.get('precip_prob') is not None:
                    try:
                        forecast_dt = dt_class.strptime(date_key, '%Y-%m-%d')
                        days_ahead = (forecast_dt - today_dt).days

                        # Only use Google for days 0-2 (0-72 hours)
                        if days_ahead <= 2:
                            precip_data[date_key] = {
                                'consensus': d.get('precip_prob', 0),
                                'source': 'Google'
                            }
                        else:
                            # For days 3+, keep AccuWeather (already set in step 2)
                            # Log when Google would disagree significantly
                            accu_val = precip_data.get(date_key, {}).get('consensus', 0)
                            google_val = d.get('precip_prob', 0)
                            if abs(google_val - accu_val) > 30:
                                logger.warning(f"[main] PRECIP RANGE CHECK: {date_key} (day {days_ahead}) - "
                                             f"Google={google_val}% vs AccuWeather={accu_val}% - Using AccuWeather")
                    except ValueError:
                        pass

        # Log precip source summary
        google_days = sum(1 for v in precip_data.values() if v.get('source') == 'Google')
        accu_days = sum(1 for v in precip_data.values() if v.get('source') == 'AccuWeather')
        om_days = sum(1 for v in precip_data.values() if v.get('source') == 'Open-Meteo')
        logger.info(f"[main] PRECIP sources: Google={google_days} (days 0-2), AccuWeather={accu_days} (days 3+), Open-Meteo={om_days}")

        pdf_path = generate_pdf_report(
            om_data=om_data,
            noaa_data=noaa_data,
            met_data=met_data,
            accu_data=accu_data,
            google_data=google_data,
            weather_com_data=weather_com_data,
            wunderground_data=wunderground_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical_hours,
            output_path=REPORT_DIR / start_time.strftime("%Y-%m") / start_time.strftime("%Y-%m-%d") / f"daily_forecast_{timestamp}.pdf",
            mid_data=mid_data,
            hrrr_data=hrrr_data,
            precip_data=precip_data,
            degraded_sources=degraded if degraded else None,
            noaa_daily_periods=noaa_daily_periods if noaa_daily_periods else None,
            report_timestamp=start_time
        )

        duration = (datetime.now(pacific) - start_time).total_seconds()

        # --- STEP 4: Summary ---
        logger.info("")
        logger.info("STEP 4: Reliability Summary...")
        logger.info("-" * 40)

        lessons = cache_mgr.get_lessons_learned()
        for provider, stats in lessons.get("provider_stats", {}).items():
            score = stats.get("reliability_score", 0)
            api_rate = stats.get("api_success_rate", 0)
            logger.info(f"  {provider}: {score:.0f}% reliability ({api_rate:.0f}% API success)")

        logger.info("")
        logger.info("=" * 60)
        logger.info("SUCCESS!")
        logger.info(f"  JSON: {json_path}")
        if pdf_path:
            logger.info(f"  PDF:  {pdf_path}")
        else:
            logger.warning("  PDF:  Generation skipped (fpdf2 not installed)")
        logger.info(f"  Providers: {len(active_sources)}/11 active")
        if degraded:
            logger.warning(f"  Degraded: {', '.join(degraded)}")
        logger.info(f"  Duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"FAILED: {e}", exc_info=True)
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"ERROR: Run failed after {(datetime.now() - start_time).total_seconds():.2f} seconds")
        logger.info("=" * 60)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
