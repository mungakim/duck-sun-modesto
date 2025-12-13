"""
Duck Sun Modesto: Uncanny Edition - Main Entry Point (Silent Edition)

Triangulates weather data, runs physics model, logs verification stats,
and outputs the PDF report. 

Decoupled from Claude Agent - pure deterministic Python.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

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

from duck_sun.providers.open_meteo import fetch_open_meteo
from duck_sun.providers.nws import NWSProvider
from duck_sun.providers.met_no import MetNoProvider
from duck_sun.providers.metar import MetarProvider
from duck_sun.providers.smoke import SmokeProvider
from duck_sun.uncanniness import UncannyEngine
from duck_sun.pdf_report import generate_pdf_report
from duck_sun.verification import TruthTracker, fetch_and_log_yesterday_actuals

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
    print(f"\n{Fore.CYAN}{'=' * 58}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   DUCK SUN MODESTO: SILENT EDITION{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   Consensus Temperature Triangulation System{Style.RESET_ALL}")
    print(f"{Fore.CYAN}   + Fog Guard + Smoke Guard{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 58}{Style.RESET_ALL}")
    print(f"{Fore.WHITE}   [SOURCES] Open-Meteo + NWS + Met.no + METAR + AQI{Style.RESET_ALL}")
    print(f"{Fore.WHITE}   [MODE] Pure Deterministic (No AI/LLM){Style.RESET_ALL}")
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
    Fetch data from all weather sources concurrently.

    Returns:
        Tuple of (om_data, nws_data, met_data, metar_text, smoke_data)
    """
    print(f"{Fore.YELLOW}[1/5]{Style.RESET_ALL} Polling Open-Meteo (GFS/ICON/GEM)...")
    logger.info("[fetch_all_sources] Fetching Open-Meteo data...")
    om_data = await fetch_open_meteo(days=8)
    print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(om_data['daily_summary'])} hourly records")
    logger.info(f"[fetch_all_sources] Open-Meteo returned {len(om_data['daily_summary'])} records")

    print(f"{Fore.YELLOW}[2/5]{Style.RESET_ALL} Polling National Weather Service...")
    logger.info("[fetch_all_sources] Fetching NWS data...")
    nws_provider = NWSProvider()
    nws_data = await nws_provider.fetch_async()
    if nws_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(nws_data)} temperature records")
        logger.info(f"[fetch_all_sources] NWS returned {len(nws_data)} records")
    else:
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL} - Using fallback")
        logger.warning("[fetch_all_sources] NWS data unavailable")

    print(f"{Fore.YELLOW}[3/5]{Style.RESET_ALL} Polling Met.no (European ECMWF)...")
    logger.info("[fetch_all_sources] Fetching Met.no data...")
    met_provider = MetNoProvider()
    met_data = await met_provider.fetch_async()
    if met_data:
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(met_data)} temperature records")
        logger.info(f"[fetch_all_sources] Met.no returned {len(met_data)} records")
    else:
        print(f"      {Fore.RED}UNAVAILABLE{Style.RESET_ALL} - Using fallback")
        logger.warning("[fetch_all_sources] Met.no data unavailable")

    print(f"{Fore.YELLOW}[4/5]{Style.RESET_ALL} Fetching KMOD Ground Truth (METAR)...")
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

    print(f"{Fore.YELLOW}[5/5]{Style.RESET_ALL} Polling Air Quality (Smoke/PM2.5)...")
    logger.info("[fetch_all_sources] Fetching smoke/AQI data...")
    smoke_provider = SmokeProvider()
    smoke_data = await smoke_provider.fetch_async(days=5)
    if smoke_data:
        max_pm = max(r['pm2_5'] for r in smoke_data)
        print(f"      {Fore.GREEN}OK{Style.RESET_ALL} - {len(smoke_data)} records (Max PM2.5: {max_pm:.1f})")
        logger.info(f"[fetch_all_sources] Smoke data: {len(smoke_data)} records, max PM2.5: {max_pm:.1f}")
        
        # Provide warning if smoke levels are elevated
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

    return om_data, nws_data, met_data, metar_raw, smoke_data


def run_consensus_model(om_data, nws_data, met_data, smoke_data=None):
    """
    Run the Consensus Temperature Model, Fog Guard, and Smoke Guard analysis.

    Args:
        om_data: Open-Meteo forecast data
        nws_data: NWS temperature data
        met_data: Met.no temperature data
        smoke_data: Smoke/AQI data from SmokeProvider

    Returns:
        Tuple of (analyzed_df, engine)
    """
    print(f"\n{Fore.CYAN}Running Consensus Temperature Model...{Style.RESET_ALL}")
    logger.info("[run_consensus_model] Starting consensus model...")

    engine = UncannyEngine()

    # Normalize and merge temperatures (now includes smoke data)
    logger.info("[run_consensus_model] Normalizing temperatures and merging smoke data...")
    df = engine.normalize_temps(om_data, nws_data, met_data, smoke_data)

    # Count sources
    sources = 1  # Open-Meteo
    if nws_data:
        sources += 1
    if met_data:
        sources += 1

    print(f"      Temperature sources contributing: {sources}/3")
    logger.info(f"[run_consensus_model] Using {sources}/3 temperature sources")
    
    if smoke_data:
        print(f"      Smoke data: {Fore.GREEN}Available{Style.RESET_ALL}")
        logger.info("[run_consensus_model] Smoke data integrated")
    else:
        print(f"      Smoke data: {Fore.YELLOW}Not available{Style.RESET_ALL}")
        logger.info("[run_consensus_model] No smoke data available")

    # Apply Fog Guard + Smoke Guard
    print(f"{Fore.CYAN}Applying Fog Guard + Smoke Guard Analysis...{Style.RESET_ALL}")
    logger.info("[run_consensus_model] Running Fog Guard + Smoke Guard analysis...")
    df_analyzed = engine.analyze_duck_curve(df)

    # Count risk levels
    critical_fog = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
    smoke_risk = len(df_analyzed[df_analyzed['risk_level'].str.contains('SMOKE', na=False)])
    moderate = len(df_analyzed[df_analyzed['risk_level'].str.contains('MODERATE', na=False)])

    # Report fog status
    if critical_fog > 0:
        print(f"      {Fore.RED}TULE FOG ALERT: {critical_fog} critical hours detected{Style.RESET_ALL}")
        logger.warning(f"[run_consensus_model] TULE FOG ALERT: {critical_fog} critical hours")
    elif moderate > 0:
        print(f"      {Fore.YELLOW}FOG RISK: {moderate} hours under monitoring{Style.RESET_ALL}")
        logger.info(f"[run_consensus_model] FOG RISK: {moderate} hours")
    else:
        print(f"      {Fore.GREEN}No fog conditions detected{Style.RESET_ALL}")
        logger.info("[run_consensus_model] No fog conditions detected")

    # Report smoke status
    if smoke_risk > 0:
        max_pm = df_analyzed['pm2_5'].max()
        print(f"      {Fore.RED}SMOKE IMPACT: {smoke_risk} hours with elevated PM2.5 (max: {max_pm:.1f}){Style.RESET_ALL}")
        logger.warning(f"[run_consensus_model] SMOKE IMPACT: {smoke_risk} hours, max PM2.5: {max_pm:.1f}")
    elif smoke_data:
        max_pm = df_analyzed['pm2_5'].max()
        if max_pm > 25:
            print(f"      {Fore.YELLOW}Light smoke present (max PM2.5: {max_pm:.1f}){Style.RESET_ALL}")
            logger.info(f"[run_consensus_model] Light smoke: max PM2.5 = {max_pm:.1f}")
        else:
            print(f"      {Fore.GREEN}Air quality good (PM2.5 < 25){Style.RESET_ALL}")
            logger.info("[run_consensus_model] Air quality good")

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
        time_str = hour['time'][-5:]  # HH:MM
        solar = hour['solar_adjusted']
        risk = hour['risk_level']

        # Color code risk levels
        if "CRITICAL" in risk:
            risk_color = Fore.RED
        elif "MODERATE" in risk:
            risk_color = Fore.YELLOW
        elif risk == "WATCH":
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
        # Highlight the winner
        color = Fore.GREEN if source == best_source else Fore.WHITE
        print(f"   {color}{source:<12} {hi_err:>5.1f}°C      {lo_err:>5.1f}°C      {count:>3}{Style.RESET_ALL}")
    
    if best_source:
        print(f"\n   Current Champion: {Fore.GREEN}{best_source}{Style.RESET_ALL}")


async def save_outputs(timestamp: str, om_data, df_analyzed, engine, metar_raw, smoke_data=None):
    """Save raw data and analysis to files, including smoke analysis."""
    ensure_directories()
    logger.info(f"[save_outputs] Saving outputs with timestamp: {timestamp}")

    # Save raw consensus data
    json_path = OUTPUT_DIR / f"solar_data_{timestamp}.json"

    # Calculate smoke summary
    smoke_summary = None
    if smoke_data:
        max_pm = max(r['pm2_5'] for r in smoke_data) if smoke_data else 0
        avg_pm = sum(r['pm2_5'] for r in smoke_data) / len(smoke_data) if smoke_data else 0
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
        logger.info("[save_outputs] No smoke data to include")

    consensus_data = {
        "generated_at": om_data["generated_at"],
        "location": "Modesto, CA",
        "sources": ["Open-Meteo", "NWS", "Met.no", "AQI"],
        "8_day_outlook": engine.get_daily_summary(df_analyzed, days=8),
        "duck_curve_tomorrow": engine.get_duck_curve_hours(df_analyzed),
        "current_metar": metar_raw,
        "smoke_analysis": smoke_summary
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(consensus_data, f, indent=2, default=str)

    print(f"\n{Fore.GREEN}Raw data saved:{Style.RESET_ALL} {json_path}")
    logger.info(f"[save_outputs] JSON saved to: {json_path}")

    return json_path


async def main():
    """Main entry point for Duck Sun Modesto: Silent Edition."""
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")

    print_banner()

    logger.info("=" * 60)
    logger.info(f"Duck Sun Modesto: Silent Edition - Run: {timestamp}")
    logger.info("=" * 60)

    try:
        # Step 1: Fetch from all sources
        print(f"{Fore.WHITE}STEP 1: Fetching Weather Data{Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 1: Fetching weather data from all sources...")
        om_data, nws_data, met_data, metar_raw, smoke_data = await fetch_all_sources()

        if not om_data:
            print(f"{Fore.RED}CRITICAL ERROR: Primary data source failed.{Style.RESET_ALL}")
            logger.error("[main] CRITICAL: Open-Meteo data fetch failed")
            return 1

        # Step 2: Run Consensus Model (now includes Smoke Guard)
        print(f"\n{Fore.WHITE}STEP 2: Consensus & Fog/Smoke Analysis{Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 2: Running consensus model with Fog Guard + Smoke Guard...")
        df_analyzed, engine = run_consensus_model(om_data, nws_data, met_data, smoke_data)

        # Step 3: Truth Tracker - Log forecasts and verify accuracy
        print(f"\n{Fore.WHITE}STEP 3: The Truth Tracker (Logging & Verification){Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 3: Running Truth Tracker verification...")
        
        tracker = TruthTracker()
        
        # 1. Log Open-Meteo Forecasts (already has daily high/low)
        count_om = 0
        if om_data.get('daily_forecast'):
            for day in om_data['daily_forecast'][:5]:  # Next 5 days
                if tracker.log_forecast("Open-Meteo", day['date'], day['high_c'], day['low_c']):
                    count_om += 1
        
        # 2. Log NWS Forecasts (aggregate hourly to daily)
        count_nws = 0
        if nws_data:
            nws_daily = NWSProvider().process_daily_high_low(nws_data)
            for date_str, stats in nws_daily.items():
                if tracker.log_forecast("NWS", date_str, stats['high'], stats['low']):
                    count_nws += 1
        
        # 3. Log Met.no Forecasts (aggregate hourly to daily)
        count_met = 0
        if met_data:
            met_daily = MetNoProvider().process_daily_high_low(met_data)
            for date_str, stats in met_daily.items():
                if tracker.log_forecast("Met.no", date_str, stats['high'], stats['low']):
                    count_met += 1
        
        print(f"   Logged predictions: OM:{count_om}, NWS:{count_nws}, Met:{count_met}")
        logger.info(f"[main] Logged forecasts - OM:{count_om}, NWS:{count_nws}, Met:{count_met}")
        
        # 4. Fetch Yesterday's Ground Truth & Show Scoreboard
        print(f"   Fetching yesterday's ground truth...")
        success = await fetch_and_log_yesterday_actuals(tracker)
        
        if success:
            print(f"   {Fore.GREEN}OK - Ground truth logged{Style.RESET_ALL}")
        else:
            print(f"   {Fore.YELLOW}Could not fetch yesterday's actuals (API lag){Style.RESET_ALL}")
        
        # 5. Display Leaderboard
        scores = tracker.get_leaderboard(days_out=1)  # Next Day Accuracy
        print_leaderboard(scores)
        
        # 6. Get Source Rankings (for PDF badges)
        source_rankings = tracker.get_source_rankings(days_out=1, last_n_days=10)
        logger.info(f"[main] Source rankings (last 10 days): {source_rankings}")
        
        # 7. Generate LEADERBOARD.md
        leaderboard_md = tracker.generate_leaderboard_markdown(days_out=1, last_n_days=10)
        leaderboard_path = Path("LEADERBOARD.md")
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
        json_path = await save_outputs(timestamp, om_data, df_analyzed, engine, metar_raw, smoke_data)

        # Step 6: Generate PDF Report (The Gold Bar)
        print(f"\n{Fore.WHITE}STEP 5: PDF Report (The Gold Bar){Style.RESET_ALL}")
        print("-" * 40)
        logger.info("[main] STEP 5: Generating PDF report...")
        
        # Count critical fog hours and smoke hours
        critical_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
        smoke_hours = len(df_analyzed[df_analyzed['risk_level'].str.contains('SMOKE', na=False)])
        max_pm = df_analyzed['pm2_5'].max() if 'pm2_5' in df_analyzed.columns else 0
        
        logger.info(f"[main] PDF context: fog_critical={critical_hours}, smoke_hours={smoke_hours}, max_pm={max_pm:.1f}")
        
        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical_hours,
            output_path=REPORT_DIR / f"daily_forecast_{timestamp}.pdf",
            source_rankings=source_rankings
        )
        
        if pdf_path:
            print(f"{Fore.GREEN}PDF report saved:{Style.RESET_ALL} {pdf_path}")
            logger.info(f"[main] PDF saved to: {pdf_path}")
        else:
            print(f"{Fore.YELLOW}PDF generation skipped (fpdf2 not installed){Style.RESET_ALL}")
            logger.warning("[main] PDF generation skipped")

        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n{Fore.GREEN}{'=' * 58}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}   SUCCESS!{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'=' * 58}{Style.RESET_ALL}")
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
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
