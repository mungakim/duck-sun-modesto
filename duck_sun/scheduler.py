"""
Duck Sun Modesto Scheduler - Silent Edition

Orchestrates the daily solar forecasting workflow:
1. Fetch and save raw solar data (Open-Meteo)
2. Generate the PDF Report (The Gold Bar) for grid schedulers

No LLM/AI agent involved. Pure deterministic python.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from duck_sun.providers.open_meteo import fetch_open_meteo
from duck_sun.providers.nws import NWSProvider
from duck_sun.providers.met_no import MetNoProvider
from duck_sun.uncanniness import UncannyEngine
from duck_sun.pdf_report import generate_pdf_report

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


def ensure_directories():
    """Create output directories if they don't exist."""
    logger.info("[ensure_directories] Ensuring output directories exist")
    OUTPUT_DIR.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)
    logger.info(f"[ensure_directories] OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
    logger.info(f"[ensure_directories] REPORT_DIR: {REPORT_DIR.absolute()}")


async def main():
    """Main scheduler entry point - Silent Edition."""
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y-%m-%d_%H-%M-%S")
    
    logger.info("=" * 60)
    logger.info(f"Duck Sun Modesto (Silent) - Run: {timestamp}")
    logger.info("=" * 60)

    try:
        ensure_directories()

        # --- STEP 1: Fetch Data ---
        logger.info("")
        logger.info("STEP 1: Fetching weather data...")
        logger.info("-" * 40)
        
        logger.info("[main] Fetching Open-Meteo data...")
        om_data = await fetch_open_meteo(days=8)
        logger.info(f"[main] Open-Meteo returned {len(om_data.get('daily_summary', []))} hourly records")
        
        logger.info("[main] Fetching NWS data...")
        nws = NWSProvider()
        nws_data = await nws.fetch_async()
        if nws_data:
            logger.info(f"[main] NWS returned {len(nws_data)} records")
        else:
            logger.warning("[main] NWS data unavailable")
        
        logger.info("[main] Fetching Met.no data...")
        met = MetNoProvider()
        met_data = await met.fetch_async()
        if met_data:
            logger.info(f"[main] Met.no returned {len(met_data)} records")
        else:
            logger.warning("[main] Met.no data unavailable")
        
        # --- STEP 2: Run Physics Engine ---
        logger.info("")
        logger.info("STEP 2: Running Uncanny Engine (Physics)...")
        logger.info("-" * 40)
        
        engine = UncannyEngine()
        logger.info("[main] Normalizing temperatures from all sources...")
        df = engine.normalize_temps(om_data, nws_data, met_data)
        
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
        
        # Save Raw Data JSON
        json_path = OUTPUT_DIR / f"solar_data_{timestamp}.json"
        logger.info(f"[main] Saving raw data to {json_path}")
        
        consensus_data = {
            "generated_at": om_data.get("generated_at", timestamp),
            "location": "Modesto, CA",
            "sources": ["Open-Meteo", "NWS", "Met.no"],
            "8_day_outlook": engine.get_daily_summary(df_analyzed, days=8),
            "duck_curve_tomorrow": engine.get_duck_curve_hours(df_analyzed)
        }
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(consensus_data, f, indent=2, default=str)
        
        logger.info(f"[main] ✓ Raw data saved to: {json_path}")
            
        # --- STEP 3: Generate PDF ---
        logger.info("")
        logger.info("STEP 3: Generating PDF Report...")
        logger.info("-" * 40)
        
        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical_hours,
            output_path=REPORT_DIR / f"daily_forecast_{timestamp}.pdf"
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("SUCCESS!")
        logger.info(f"  JSON: {json_path}")
        if pdf_path:
            logger.info(f"  PDF:  {pdf_path}")
        else:
            logger.warning("  PDF:  Generation skipped (fpdf2 not installed)")
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
