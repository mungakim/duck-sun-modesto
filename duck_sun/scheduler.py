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
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Core providers
from duck_sun.providers.open_meteo import fetch_open_meteo, fetch_hrrr_forecast
from duck_sun.providers.nws import NWSProvider
from duck_sun.providers.met_no import MetNoProvider
from duck_sun.providers.accuweather import AccuWeatherProvider
from duck_sun.providers.weathercom import WeatherComProvider
from duck_sun.providers.mid_org import MIDOrgProvider
from duck_sun.providers.metar import MetarProvider
from duck_sun.providers.smoke import SmokeProvider

# Processing
from duck_sun.uncanniness import UncannyEngine
from duck_sun.pdf_report import generate_pdf_report

# Resilience infrastructure
from duck_sun.resilience import with_retry, RetryConfig, categorize_error
from duck_sun.cache_manager import CacheManager, FetchResult

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


def ensure_directories():
    """Create output directories if they don't exist."""
    logger.info("[ensure_directories] Ensuring output directories exist")
    OUTPUT_DIR.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "cache").mkdir(exist_ok=True)
    logger.info(f"[ensure_directories] OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.info(f"[ensure_directories] REPORT_DIR: {REPORT_DIR.absolute()}")


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

    logger.info("[fetch_all_providers] Starting fetch from 9 providers...")

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

    # 3. NWS (highest weight - 5x)
    logger.info("[fetch_all_providers] Fetching NWS...")

    async def _fetch_nws():
        nws = NWSProvider()
        return await nws.fetch_async()

    results["nws"] = await fetch_with_retry("nws", _fetch_nws, cache_mgr)

    # 4. Met.no (ECMWF model)
    logger.info("[fetch_all_providers] Fetching Met.no...")

    async def _fetch_met():
        met = MetNoProvider()
        return await met.fetch_async()

    results["met_no"] = await fetch_with_retry("met_no", _fetch_met, cache_mgr)

    # 5. AccuWeather (commercial - weight 3x)
    logger.info("[fetch_all_providers] Fetching AccuWeather...")

    async def _fetch_accu():
        accu = AccuWeatherProvider()
        return await accu.fetch_forecast()

    results["accuweather"] = await fetch_with_retry("accuweather", _fetch_accu, cache_mgr)

    # 6. Weather.com (manual cache mode)
    logger.info("[fetch_all_providers] Fetching Weather.com...")

    async def _fetch_weathercom():
        wc = WeatherComProvider()
        return await wc.fetch_forecast()

    results["weathercom"] = await fetch_with_retry("weathercom", _fetch_weathercom, cache_mgr)

    # 7. MID.org (local ground truth)
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

        # --- STEP 1: Fetch ALL Data Sources ---
        logger.info("")
        logger.info("STEP 1: Fetching weather data from ALL 9 providers...")
        logger.info("-" * 40)

        results = await fetch_all_providers(cache_mgr)

        # Extract data from results
        om_data = results["open_meteo"].data
        hrrr_data = results["hrrr"].data
        nws_data = results["nws"].data
        met_data = results["met_no"].data
        accu_data = results["accuweather"].data
        weathercom_data = results["weathercom"].data
        mid_data = results["mid_org"].data
        metar_data = results["metar"].data
        smoke_data = results["smoke"].data

        # Check critical provider
        if om_data is None or not om_data:
            logger.error("CRITICAL: Open-Meteo data unavailable - cannot continue")
            return 1

        # --- SPECIAL HANDLING FOR NWS PERIOD DATA ---
        # Fetch the Period-based forecast for website alignment
        nws_daily_periods = {}
        try:
            nws_periods_provider = NWSProvider()
            await nws_periods_provider.fetch_forecast_periods()
            nws_daily_periods = nws_periods_provider.get_daily_high_low()
            logger.info(f"[main] NWS Period Daily Stats: {len(nws_daily_periods)} days")
            for date_key, stats in list(nws_daily_periods.items())[:3]:
                logger.info(f"[main]   {date_key}: Hi={stats.get('high_f')}F, Lo={stats.get('low_f')}F")
        except Exception as e:
            logger.warning(f"[main] NWS period fetch failed: {e}")

        # --- STEP 2: Run Physics Engine ---
        logger.info("")
        logger.info("STEP 2: Running Uncanny Engine (Physics)...")
        logger.info("-" * 40)

        engine = UncannyEngine()
        logger.info("[main] Normalizing temperatures from all sources...")

        # Pass additional sources to normalize_temps if available
        df = engine.normalize_temps(
            om_data,
            nws_data if nws_data else None,
            met_data if met_data else None,
            accu_data=accu_data if accu_data else None,
            weathercom_data=weathercom_data if weathercom_data else None,
            mid_data=mid_data if mid_data else None
        )

        logger.info("[main] Analyzing duck curve and fog risk...")
        df_analyzed = engine.analyze_duck_curve(df)

        # Count risk levels
        critical_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
        moderate_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('MODERATE', na=False)])

        if critical_hours > 0:
            logger.warning(f"[main] TULE FOG ALERT: {critical_hours} critical hours detected")
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

        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            accu_data=accu_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical_hours,
            output_path=REPORT_DIR / start_time.strftime("%Y-%m") / start_time.strftime("%Y-%m-%d") / f"daily_forecast_{timestamp}.pdf",
            weathercom_data=weathercom_data,
            mid_data=mid_data,
            hrrr_data=hrrr_data,
            degraded_sources=degraded if degraded else None,
            nws_daily_periods=nws_daily_periods if nws_daily_periods else None
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
        logger.info(f"  Providers: {len(active_sources)}/9 active")
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
