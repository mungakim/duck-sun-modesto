"""
Duck Sun Modesto: WEIGHTED ENSEMBLE Architecture

Triangulates weather data from 8 sources using weighted ensemble consensus,
runs physics model with narrative override, logs verification stats,
and outputs the PDF report with variance alerts.

Sources: Open-Meteo + NWS + Met.no + AccuWeather + Weather.com + MID.org + METAR + Smoke
Weights: NWS(5x) > AccuWeather(3x) = Met.no(3x) > Weather.com(2x) = MID.org(2x) > Open-Meteo(1x)
Physics: Fog Guard + Smoke Guard + NWS Narrative Override
Variance: Warn-only alerts for >10°F spread (never blocks)

RELIABILITY IS KING - Consistent, accurate values every time.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Duck Sun Modesto Daily Forecaster - Weighted Ensemble Architecture'
    )
    # Future CLI args can be added here if needed
    return parser.parse_args()

# Initialize colorama for Windows terminal colors
try:
    from colorama import init, Fore, Style
    init()
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    class Fore:
        CYAN = YELLOW = GREEN = RED = WHITE = RESET = ""
    class Style:
        BRIGHT = RESET_ALL = ""

from duck_sun.providers.open_meteo import fetch_open_meteo, fetch_hrrr_forecast, get_precipitation_probabilities
from duck_sun.providers.nws import NWSProvider
from duck_sun.providers.met_no import MetNoProvider
from duck_sun.providers.metar import MetarProvider
from duck_sun.providers.smoke import SmokeProvider
from duck_sun.providers.accuweather import AccuWeatherProvider
from duck_sun.providers.weathercom import WeatherComProvider
from duck_sun.providers.mid_org import MIDOrgProvider
from duck_sun.uncanniness import UncannyEngine
from duck_sun.pdf_report import generate_pdf_report
from duck_sun.verification import TruthTracker, fetch_yesterday_actuals

# Load environment variables
load_dotenv()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/duck_sun.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Output directories
OUTPUT_DIR = Path("outputs")
REPORT_DIR = Path("reports")


def print_banner():
    """Print the system banner."""
    print(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   DUCK SUN MODESTO: WEIGHTED ENSEMBLE ARCHITECTURE{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   Reliability-First Temperature Consensus System{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   + Fog Guard + Smoke Guard + Narrative Override{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}   [SOURCES] Open-Meteo + HRRR + NWS + Met.no + AccuWeather + Weather.com + MID.org{Style.RESET_ALL}")
    print(f"{Fore.WHITE}   [WEIGHTS] NWS(5x) > Accu(3x) = Met(3x) > WC(2x) = MID(2x) > OM(1x){Style.RESET_ALL}")
    print(f"{Fore.WHITE}   [VARIANCE] Warn-only alerts for >10°F spread (never blocks){Style.RESET_ALL}")
    print()


def ensure_directories():
    """Create output directories if they don't exist."""
    logger.info("[ensure_directories] Creating output directories...")
    OUTPUT_DIR.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)
    logger.info(f"[ensure_directories] OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.info(f"[ensure_directories] REPORT_DIR: {REPORT_DIR.absolute()}")


async def fetch_all_sources():
    """
    Fetch data from all weather sources (9 total).

    Returns:
        Tuple of (om_data, nws_data, nws_text, met_data, metar_raw, accu_data, smoke_data, weathercom_data, mid_data, hrrr_data)
    """
    print(f"{Fore.YELLOW}[1/9]{Style.RESET_ALL} Polling Open-Meteo (GFS/ICON/GEM)...")
    logger.info("[fetch_all_sources] Fetching Open-Meteo data...")
    om_data = await fetch_open_meteo(days=8)
    print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(om_data['daily_summary'])} hourly records")
    logger.info(f"[fetch_all_sources] Open-Meteo returned {len(om_data['daily_summary'])} records")

    print(f"{Fore.YELLOW}[2/9]{Style.RESET_ALL} Polling HRRR Model (3km, 15-min updates)...")
    logger.info("[fetch_all_sources] Fetching HRRR data...")
    hrrr_data = await fetch_hrrr_forecast()
    if hrrr_data:
        fog_hours = sum(1 for h in hrrr_data.get('hourly', []) if h.get('is_fog'))
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(hrrr_data.get('hourly', []))} hourly records (Fog hours: {fog_hours})")
        logger.info(f"[fetch_all_sources] HRRR returned {len(hrrr_data.get('hourly', []))} records")
    else:
        print(f"      {Fore.YELLOW}UNAVAILABLE{Style.RESET_ALL} - Using other models")
        logger.warning("[fetch_all_sources] HRRR data unavailable")

    print(f"{Fore.YELLOW}[3/9]{Style.RESET_ALL} Polling National Weather Service...")
    logger.info("[fetch_all_sources] Fetching NWS data...")
    nws_provider = NWSProvider()
    nws_data = await nws_provider.fetch_async()
    nws_text = await nws_provider.fetch_text_forecast()

    # Fetch NWS Period data (matches website exactly)
    await nws_provider.fetch_forecast_periods()
    nws_daily_periods = nws_provider.get_daily_high_low()

    if nws_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(nws_data)} temperature records")
        logger.info(f"[fetch_all_sources] NWS returned {len(nws_data)} records")
    else:
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL} - Using fallback")
        logger.warning("[fetch_all_sources] NWS data unavailable")

    if nws_daily_periods:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(nws_daily_periods)} days period data (Website Match)")
        logger.info(f"[fetch_all_sources] NWS period daily stats: {len(nws_daily_periods)} days")
    else:
        logger.warning("[fetch_all_sources] NWS period data unavailable")

    if nws_text:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(nws_text)} text forecast periods (Narrative)")
        logger.info(f"[fetch_all_sources] NWS text forecast: {len(nws_text)} periods")
    else:
        logger.warning("[fetch_all_sources] NWS text forecast unavailable")

    print(f"{Fore.YELLOW}[4/9]{Style.RESET_ALL} Polling Met.no (European ECMWF)...")
    logger.info("[fetch_all_sources] Fetching Met.no data...")
    met_provider = MetNoProvider()
    met_data = await met_provider.fetch_async()
    if met_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(met_data)} temperature records")
        logger.info(f"[fetch_all_sources] Met.no returned {len(met_data)} records")
    else:
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL} - Using fallback")
        logger.warning("[fetch_all_sources] Met.no data unavailable")

    print(f"{Fore.YELLOW}[5/9]{Style.RESET_ALL} Polling AccuWeather (Commercial)...")
    logger.info("[fetch_all_sources] Fetching AccuWeather data...")
    accu_provider = AccuWeatherProvider()
    accu_data = await accu_provider.fetch_forecast()
    if accu_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(accu_data)} daily forecasts")
        logger.info(f"[fetch_all_sources] AccuWeather returned {len(accu_data)} records")
    else:
        print(f"      {Fore.YELLOW}UNAVAILABLE{Style.RESET_ALL} - Quota exceeded or no API key")
        logger.warning("[fetch_all_sources] AccuWeather data unavailable")

    print(f"{Fore.YELLOW}[6/9]{Style.RESET_ALL} Polling Weather.com (Baseline)...")
    logger.info("[fetch_all_sources] Fetching Weather.com baseline data...")
    weathercom_provider = WeatherComProvider()
    weathercom_data = await weathercom_provider.fetch_forecast()
    if weathercom_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(weathercom_data)} daily forecasts (baseline)")
        logger.info(f"[fetch_all_sources] Weather.com returned {len(weathercom_data)} records")
    else:
        print(f"      {Fore.YELLOW}UNAVAILABLE{Style.RESET_ALL} - Using other sources")
        logger.warning("[fetch_all_sources] Weather.com data unavailable")

    print(f"{Fore.YELLOW}[7/9]{Style.RESET_ALL} Polling MID.org (Local Modesto)...")
    logger.info("[fetch_all_sources] Fetching MID.org local data...")
    mid_provider = MIDOrgProvider()
    mid_data = await mid_provider.fetch_48hr_summary()
    if mid_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - Local microclimate data")
        logger.info(f"[fetch_all_sources] MID.org data retrieved")
    else:
        print(f"      {Fore.YELLOW}UNAVAILABLE{Style.RESET_ALL} - JS-rendered (pending enhancement)")
        logger.info("[fetch_all_sources] MID.org data unavailable (expected - JS-rendered)")

    print(f"{Fore.YELLOW}[8/9]{Style.RESET_ALL} Fetching KMOD Ground Truth (METAR)...")
    logger.info("[fetch_all_sources] Fetching METAR data...")
    metar_provider = MetarProvider()
    metar_raw = await metar_provider.fetch_async()
    if metar_raw:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL}")
        logger.info("[fetch_all_sources] METAR data retrieved")
    else:
        metar_raw = "UNAVAILABLE"
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL}")
        logger.warning("[fetch_all_sources] METAR data unavailable")

    print(f"{Fore.YELLOW}[9/9]{Style.RESET_ALL} Polling Air Quality (Smoke/PM2.5)...")
    logger.info("[fetch_all_sources] Fetching smoke/AQI data...")
    smoke_provider = SmokeProvider()
    smoke_data = await smoke_provider.fetch_async(days=5)
    if smoke_data:
        max_pm = max(r['pm2_5'] for r in smoke_data)
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(smoke_data)} records (Max PM2.5: {max_pm:.1f})")
        logger.info(f"[fetch_all_sources] Smoke data: {len(smoke_data)} records, max PM2.5: {max_pm:.1f}")

        if max_pm > 100:
            print(f"      {Fore.RED}⚠ SMOKE ALERT: PM2.5 > 100 ug/m3{Style.RESET_ALL}")
            logger.warning(f"[fetch_all_sources] SMOKE ALERT: PM2.5 = {max_pm:.1f} ug/m3")
        elif max_pm > 50:
            print(f"      {Fore.YELLOW}⚠ Moderate smoke levels detected{Style.RESET_ALL}")
            logger.info(f"[fetch_all_sources] Moderate smoke: PM2.5 = {max_pm:.1f} ug/m3")
    else:
        smoke_data = None
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL}")
        logger.warning("[fetch_all_sources] Smoke data unavailable")

    return om_data, nws_data, nws_text, met_data, metar_raw, accu_data, smoke_data, weathercom_data, mid_data, hrrr_data, nws_daily_periods


def run_consensus_model(om_data, nws_data, met_data, accu_data, weathercom_data, mid_data, smoke_data, nws_text):
    """
    Run the WEIGHTED ENSEMBLE Consensus Model with Narrative Override.

    Args:
        om_data: Open-Meteo forecast data
        nws_data: NWS temperature data
        met_data: Met.no temperature data
        accu_data: AccuWeather daily forecasts
        weathercom_data: Weather.com baseline (via wttr.in)
        mid_data: MID.org local data (if available)
        smoke_data: Smoke/AQI data
        nws_text: NWS text forecast for narrative override

    Returns:
        Tuple of (analyzed_df, engine)
    """
    print(f"\n{Fore.CYAN}Running WEIGHTED ENSEMBLE Consensus Model...{Style.RESET_ALL}")
    logger.info("[run_consensus_model] Starting weighted ensemble consensus model...")

    engine = UncannyEngine()

    # Normalize and merge temperatures from ALL sources
    logger.info("[run_consensus_model] Building weighted ensemble (NWS 5x > Accu 3x > Met 3x > WC 2x > MID 2x > OM 1x)...")
    df = engine.normalize_temps(
        om_data, nws_data, met_data,
        accu_data=accu_data,
        weathercom_data=weathercom_data,
        mid_data=mid_data,
        smoke_data=smoke_data
    )

    # Count sources
    sources = 1  # Open-Meteo always available
    if nws_data:
        sources += 1
    if met_data:
        sources += 1
    if accu_data:
        sources += 1
    if weathercom_data:
        sources += 1
    if mid_data:
        sources += 1

    print(f"      Temperature sources contributing: {sources}/6")
    logger.info(f"[run_consensus_model] Using {sources}/6 temperature sources")

    # === VARIANCE REPORT ===
    var_report = engine.get_variance_report()
    variance_counts = var_report.get('variance_counts', {})
    critical_variance = variance_counts.get('CRITICAL', 0)
    moderate_variance = variance_counts.get('MODERATE', 0)

    if critical_variance > 0:
        print(f"      {Fore.RED}VARIANCE WARNING: {critical_variance} hours with >10°F spread{Style.RESET_ALL}")
        logger.warning(f"[run_consensus_model] VARIANCE WARNING: {critical_variance} hours CRITICAL")
    elif moderate_variance > 0:
        print(f"      {Fore.YELLOW}Variance note: {moderate_variance} hours with 5-10°F spread{Style.RESET_ALL}")
        logger.info(f"[run_consensus_model] Moderate variance: {moderate_variance} hours")
    else:
        print(f"      {Fore.GREEN}Source agreement: LOW variance across all hours{Style.RESET_ALL}")
        logger.info("[run_consensus_model] LOW variance - sources agree well")

    print(f"      Avg confidence: {var_report.get('avg_confidence', 0):.2f}")

    # Apply Fog Guard + Smoke Guard + Narrative Override
    print(f"{Fore.CYAN}Applying Fog Guard + Smoke Guard + Narrative Override...{Style.RESET_ALL}")
    logger.info("[run_consensus_model] Running physics engine with narrative override...")
    df_analyzed = engine.analyze_duck_curve(df, nws_text_data=nws_text)

    # Count risk levels
    critical_fog = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
    smoke_risk = len(df_analyzed[df_analyzed['risk_level'].str.contains('SMOKE', na=False)])
    moderate = len(df_analyzed[df_analyzed['risk_level'].str.contains('MODERATE', na=False)])

    if critical_fog > 0:
        print(f"      {Fore.RED}TULE FOG ALERT: {critical_fog} critical hours detected{Style.RESET_ALL}")
        logger.warning(f"[run_consensus_model] TULE FOG ALERT: {critical_fog} critical hours")
    elif moderate > 0:
        print(f"      {Fore.YELLOW}FOG RISK: {moderate} hours under monitoring{Style.RESET_ALL}")
        logger.info(f"[run_consensus_model] FOG RISK: {moderate} hours")
    else:
        print(f"      {Fore.GREEN}No fog conditions detected{Style.RESET_ALL}")
        logger.info("[run_consensus_model] No fog conditions detected")

    if smoke_risk > 0:
        max_pm = df_analyzed['pm2_5'].max()
        print(f"      {Fore.RED}SMOKE IMPACT: {smoke_risk} hours with elevated PM2.5 (max: {max_pm:.1f}){Style.RESET_ALL}")
        logger.warning(f"[run_consensus_model] SMOKE IMPACT: {smoke_risk} hours, max PM2.5: {max_pm:.1f}")

    return df_analyzed, engine


def print_8day_outlook(engine, df_analyzed):
    """Print the 8-day temperature outlook table."""
    print(f"\n{Fore.CYAN}{'=' * 50}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}8-DAY CONSENSUS TEMPERATURE OUTLOOK{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 50}{Style.RESET_ALL}")

    daily = engine.get_daily_summary(df_analyzed, days=8)

    print(f"{'Date':<12} {'Temp (C)':<10} {'Solar (adj)':<12} {'Clouds'}")
    print("-" * 50)

    for day in daily:
        print(f"{day['date']:<12} {day['temp_consensus_c']:>6.1f}C    "
              f"{day['avg_solar_adjusted']:>8.0f} W/m²  {day['avg_cloud_cover']:>3.0f}%")


def print_duck_curve(engine, df_analyzed):
    """Print tomorrow's duck curve forecast."""
    print(f"\n{Fore.CYAN}{'=' * 50}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}TOMORROW'S DUCK CURVE (HE09-HE16){Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 50}{Style.RESET_ALL}")

    duck_hours = engine.get_duck_curve_hours(df_analyzed)

    if not duck_hours:
        print("No duck curve data available for tomorrow")
        return

    print(f"{'Hour':<8} {'Solar (W/m²)':<14} {'Risk Level'}")
    print("-" * 50)

    for hour in duck_hours:
        time_str = hour['time'][-5:]
        solar = hour['solar_adjusted']
        risk = hour['risk_level']

        if "CRITICAL" in risk:
            risk_color = Fore.RED
        elif "MODERATE" in risk or "HIGH" in risk:
            risk_color = Fore.YELLOW
        else:
            risk_color = Fore.GREEN

        print(f"HE{time_str[:2]}     {solar:>8.0f}       {risk_color}{risk}{Style.RESET_ALL}")


def print_leaderboard(scores, best_source: str = None):
    """Print the accuracy leaderboard."""
    if not scores:
        print(f"   {Fore.YELLOW}Not enough data for leaderboard yet (need 24h history){Style.RESET_ALL}")
        return
    
    print(f"\n   {Fore.CYAN}🏆 ACCURACY LEADERBOARD (Next Day Forecast){Style.RESET_ALL}")
    print(f"   {'Source':<12} {'High Err':<10} {'Low Err':<10} {'Samples'}")
    print(f"   {'-'*45}")
    
    if best_source is None and scores:
        best_source = scores[0][0]
    
    for row in scores:
        source, count, hi_err, lo_err = row
        color = Fore.GREEN if source == best_source else Fore.WHITE
        print(f"   {color}{source:<12} {hi_err:>5.1f}°C      {lo_err:>5.1f}°C      {count:>3}{Style.RESET_ALL}")
    
    if best_source:
        print(f"\n   Current Champion: {Fore.GREEN}{best_source}{Style.RESET_ALL}")


async def save_outputs(timestamp: str, om_data, df_analyzed, engine, metar_raw, accu_data, weathercom_data, mid_data, smoke_data=None):
    """Save raw data and analysis to files."""
    ensure_directories()
    logger.info(f"[save_outputs] Saving outputs with timestamp: {timestamp}")

    json_path = OUTPUT_DIR / f"solar_data_{timestamp}.json"

    # Calculate smoke summary
    smoke_summary = None
    if smoke_data:
        max_pm = max(r['pm2_5'] for r in smoke_data)
        avg_pm = sum(r['pm2_5'] for r in smoke_data) / len(smoke_data)
        smoke_hours = len([r for r in smoke_data if r['pm2_5'] > 50])
        smoke_summary = {
            "max_pm2_5": round(max_pm, 1),
            "avg_pm2_5": round(avg_pm, 1),
            "hours_above_50": smoke_hours,
            "data_available": True
        }
        logger.info(f"[save_outputs] Smoke summary: max={max_pm:.1f}, avg={avg_pm:.1f}, hours>50={smoke_hours}")
    else:
        smoke_summary = {"data_available": False}

    consensus_data = {
        "generated_at": om_data["generated_at"],
        "location": "Modesto, CA",
        "architecture": "Weighted Ensemble (Reliability-First)",
        "sources": ["Open-Meteo", "NWS", "Met.no", "AccuWeather", "Weather.com", "MID.org", "AQI"],
        "weights": {"NWS": 5, "AccuWeather": 3, "Met.no": 3, "Weather.com": 2, "MID.org": 2, "Open-Meteo": 1},
        "variance_report": engine.get_variance_report() if hasattr(engine, 'get_variance_report') else {},
        "8_day_outlook": engine.get_daily_summary(df_analyzed, days=8),
        "duck_curve_tomorrow": engine.get_duck_curve_hours(df_analyzed),
        "current_metar": metar_raw,
        "accuweather_available": accu_data is not None,
        "weathercom_available": weathercom_data is not None,
        "mid_org_available": mid_data is not None,
        "smoke_analysis": smoke_summary
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(consensus_data, f, indent=2, default=str)

    print(f"\n{Fore.GREEN}Raw data saved:{Style.RESET_ALL} {json_path}")
    logger.info(f"[save_outputs] JSON saved to: {json_path}")

    return json_path


async def main(args=None):
    """Main entry point for Duck Sun Modesto: WEIGHTED ENSEMBLE Architecture."""
    pacific = ZoneInfo("America/Los_Angeles")
    start_time = datetime.now(pacific)
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")

    print_banner()

    logger.info("=" * 60)
    logger.info(f"Duck Sun Modesto: WEIGHTED ENSEMBLE Architecture - Run: {timestamp}")
    logger.info("=" * 60)

    try:
        # Step 1: Fetch from all sources
        print(f"{Fore.WHITE}STEP 1: Fetching Weather Data (9 Sources){Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 1: Fetching weather data from all sources...")
        (om_data, nws_data, nws_text, met_data, metar_raw,
         accu_data, smoke_data, weathercom_data, mid_data, hrrr_data, nws_daily_periods) = await fetch_all_sources()

        if not om_data:
            print(f"{Fore.RED}CRITICAL ERROR: Primary data source failed.{Style.RESET_ALL}")
            logger.error("[main] CRITICAL: Open-Meteo data fetch failed")
            return 1

        # Step 2: Run WEIGHTED ENSEMBLE Consensus Model with Narrative Override
        print(f"\n{Fore.WHITE}STEP 2: WEIGHTED ENSEMBLE Consensus & Physics Analysis{Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 2: Running weighted ensemble consensus model...")
        df_analyzed, engine = run_consensus_model(
            om_data, nws_data, met_data,
            accu_data, weathercom_data, mid_data,
            smoke_data, nws_text
        )

        # Step 3: Truth Tracker
        print(f"\n{Fore.WHITE}STEP 3: The Truth Tracker (Logging & Verification){Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 3: Running Truth Tracker verification...")
        
        tracker = TruthTracker()
        
        # Log forecasts from all sources
        count_om = 0
        if om_data.get('daily_forecast'):
            for day in om_data['daily_forecast'][:5]:
                if tracker.log_forecast("Open-Meteo", day['date'], day['high_c'], day['low_c']):
                    count_om += 1
        
        count_nws = 0
        if nws_data:
            nws_daily = NWSProvider().process_daily_high_low(nws_data)
            for date_str, stats in nws_daily.items():
                if tracker.log_forecast("NWS", date_str, stats['high'], stats['low']):
                    count_nws += 1
        
        count_met = 0
        if met_data:
            met_daily = MetNoProvider().process_daily_high_low(met_data)
            for date_str, stats in met_daily.items():
                if tracker.log_forecast("Met.no", date_str, stats['high'], stats['low']):
                    count_met += 1
        
        # Log AccuWeather forecasts
        count_accu = 0
        if accu_data:
            for day in accu_data:
                if tracker.log_forecast("AccuWeather", day['date'], day['high_c'], day['low_c']):
                    count_accu += 1

        # Log Weather.com forecasts (baseline)
        count_wc = 0
        if weathercom_data:
            for day in weathercom_data:
                if tracker.log_forecast("Weather.com", day['date'], day['high_c'], day['low_c']):
                    count_wc += 1

        # Log MID.org if available
        count_mid = 0
        if mid_data and mid_data.get('high_c') is not None:
            if tracker.log_forecast("MID.org", mid_data['date'], mid_data['high_c'], mid_data['low_c']):
                count_mid = 1

        print(f"   Logged predictions: OM:{count_om}, NWS:{count_nws}, Met:{count_met}, Accu:{count_accu}, WC:{count_wc}, MID:{count_mid}")
        logger.info(f"[main] Logged forecasts - OM:{count_om}, NWS:{count_nws}, Met:{count_met}, Accu:{count_accu}, WC:{count_wc}, MID:{count_mid}")
        
        # Fetch Ground Truth
        print(f"   Fetching yesterday's ground truth...")
        actuals = await fetch_yesterday_actuals()
        
        if actuals:
            condition = "Clear"  # Default condition
            tracker.ingest_actuals(
                actuals['date'], 
                actuals['high'], 
                actuals['low'], 
                condition,
                actuals.get('precip', 0.0)
            )
            print(f"   {Fore.GREEN}OK - Ground truth logged ({actuals['date']}: Hi={actuals['high']:.1f}C, Lo={actuals['low']:.1f}C){Style.RESET_ALL}")
            logger.info(f"[main] Logged actuals: {actuals['date']} Hi={actuals['high']}C Lo={actuals['low']}C")
        else:
            print(f"   {Fore.YELLOW}Could not fetch yesterday's actuals (API lag){Style.RESET_ALL}")
        
        # Display Leaderboard
        scores = tracker.get_leaderboard(days_back=10)
        print_leaderboard(scores)
        
        
        # Generate LEADERBOARD.md
        leaderboard_path = Path("LEADERBOARD.md")
        report = tracker.get_verification_report(days_back=10)
        leaderboard_md = f"""# Duck Sun Modesto - Accuracy Leaderboard

## 10-Day Performance Report

| Rank | Source | High Error | Low Error | Samples |
|------|--------|------------|-----------|---------|
"""
        for source, data in report.get('sources', {}).items():
            leaderboard_md += f"| #{data.get('rank', '-')} | {source} | {data.get('high_mae', 0):.2f}°C | {data.get('low_mae', 0):.2f}°C | {data.get('comparisons', 0)} |\n"
        
        if not report.get('sources'):
            leaderboard_md += "| - | No data yet | - | - | - |\n"
        
        leaderboard_md += f"\n---\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n"
        
        with open(leaderboard_path, "w", encoding="utf-8") as f:
            f.write(leaderboard_md)
        print(f"   {Fore.GREEN}LEADERBOARD.md updated{Style.RESET_ALL}")
        logger.info(f"[main] LEADERBOARD.md saved to: {leaderboard_path}")
        
        tracker.close()

        # Step 4: Display Results
        print_8day_outlook(engine, df_analyzed)
        print_duck_curve(engine, df_analyzed)

        # Step 5: Save Outputs
        print(f"\n{Fore.WHITE}STEP 4: Saving Outputs{Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 4: Saving outputs...")
        json_path = await save_outputs(timestamp, om_data, df_analyzed, engine, metar_raw, accu_data, weathercom_data, mid_data, smoke_data)

        # Step 6: Generate PDF Report
        print(f"\n{Fore.WHITE}STEP 5: PDF Report (Weighted Consensus){Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 5: Generating PDF report...")

        critical_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])

        # Calculate precipitation consensus from all sources (HRRR weighted highest)
        precip_data = get_precipitation_probabilities(om_data, hrrr_data, weathercom_data, accu_data)
        logger.info(f"[main] Precipitation data aggregated for {len(precip_data)} days")

        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            accu_data=accu_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical_hours,
            output_path=REPORT_DIR / f"daily_forecast_{timestamp}.pdf",
            weathercom_data=weathercom_data,
            mid_data=mid_data,
            hrrr_data=hrrr_data,
            precip_data=precip_data,
            nws_daily_periods=nws_daily_periods
        )
        
        if pdf_path:
            print(f"{Fore.GREEN}PDF report saved:{Style.RESET_ALL} {pdf_path}")
            logger.info(f"[main] PDF saved to: {pdf_path}")
        else:
            print(f"{Fore.YELLOW}PDF generation skipped (fpdf2 not installed){Style.RESET_ALL}")
            logger.warning("[main] PDF generation skipped")

        # Success summary
        end_time = datetime.now(pacific)
        duration = (end_time - start_time).total_seconds()

        print(f"\n{Fore.GREEN}{'=' * 60}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}   SUCCESS!{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'=' * 60}{Style.RESET_ALL}")
        print(f"   Raw data: {json_path}")
        if pdf_path:
            print(f"   PDF: {pdf_path}")
        print(f"   Duration: {duration:.2f} seconds")
        print()

        return 0

    except Exception as e:
        logger.error(f"FAILED: {e}", exc_info=True)
        print(f"\n{Fore.RED}ERROR: {e}{Style.RESET_ALL}")
        return 1


if __name__ == "__main__":
    args = parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
