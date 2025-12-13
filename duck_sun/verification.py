"""
The Truth Tracker - Verification System for Duck Sun Modesto

Tracks forecast accuracy by logging predictions and comparing them
against observed historical data. Uses a SQLite database to maintain
a permanent leaderboard of which source is most accurate.

Architecture:
1. THE VAULT (Database): SQLite file storing all forecasts and actuals
2. THE LOGGER (Input): Saves forecasts from each source with run_date
3. THE AUDITOR (Output): Grades each source against ground truth (MAE)

Ground Truth Source:
- Uses Open-Meteo's Archive API which combines:
  - Station data (KMOD Modesto Airport)
  - Reanalysis data (ERA5 satellite + models)
  - This ensures we never have "missing" verification data

Usage:
    from duck_sun.verification import TruthTracker, fetch_and_log_yesterday_actuals

    tracker = TruthTracker()
    tracker.log_forecast("Open-Meteo", "2025-12-13", high_c=15.0, low_c=5.0)
    
    await fetch_and_log_yesterday_actuals(tracker)
    
    leaderboard = tracker.get_leaderboard(days_out=1)
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, TypedDict, Tuple

logger = logging.getLogger(__name__)

# Persistent database file (at project root)
DB_PATH = Path("verification.db")


class DailyHighLow(TypedDict):
    """Daily temperature summary extracted from hourly data."""
    date: str
    high: float
    low: float


class VerificationReport(TypedDict):
    """Verification report structure."""
    generated_at: str
    verified_days: int
    sources: Dict[str, Dict[str, Any]]


class LeaderboardEntry(TypedDict):
    """Single entry in the accuracy leaderboard."""
    source: str
    comparisons: int
    high_error_mae: float
    low_error_mae: float
    combined_mae: float
    rank: int


class TruthTracker:
    """
    Manages the forecast verification database.
    
    Stores:
    1. Forecasts: What each source predicted (with run_date)
    2. Observations: What actually happened (ground truth)
    
    Calculates Mean Absolute Error (MAE) for each source to determine
    which provider is most accurate for the Modesto microclimate.
    """
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the TruthTracker.
        
        Args:
            db_path: Path to SQLite database. Defaults to verification.db in project root.
        """
        self.db_path = db_path or DB_PATH
        logger.info(f"[TruthTracker] Initializing with database: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        self._init_db()
        
        logger.info("[TruthTracker] Database connection established")

    def _init_db(self):
        """Initialize the database schema."""
        logger.debug("[TruthTracker] Creating database schema if needed...")
        
        cursor = self.conn.cursor()
        
        # Table 1: FORECASTS - What the models predicted
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,         -- 'NWS', 'Open-Meteo', 'Met.no'
                run_date TEXT NOT NULL,       -- When prediction was made (YYYY-MM-DD)
                target_date TEXT NOT NULL,    -- Date being predicted (YYYY-MM-DD)
                days_out INTEGER NOT NULL,    -- 0=Today, 1=Tomorrow, etc.
                pred_high_c REAL,             -- Predicted high (Celsius)
                pred_low_c REAL,              -- Predicted low (Celsius)
                UNIQUE(source, run_date, target_date)
            )
        ''')

        # Table 2: OBSERVATIONS - What actually happened (Ground Truth)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                date TEXT PRIMARY KEY,        -- YYYY-MM-DD
                actual_high_c REAL,           -- Observed high (Celsius)
                actual_low_c REAL,            -- Observed low (Celsius)
                actual_precip_mm REAL         -- Observed precipitation (mm)
            )
        ''')
        
        # Index for faster leaderboard queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_forecasts_lookup 
            ON forecasts(source, target_date, days_out)
        ''')
        
        self.conn.commit()
        logger.debug("[TruthTracker] Schema initialized")

    def log_forecast(
        self, 
        source: str, 
        target_date: str, 
        high_c: float, 
        low_c: float
    ) -> bool:
        """
        Log a forecast prediction into the database.
        
        Args:
            source: Provider name ('Open-Meteo', 'NWS', 'Met.no')
            target_date: Date being forecast (YYYY-MM-DD)
            high_c: Predicted high temperature (Celsius)
            low_c: Predicted low temperature (Celsius)
            
        Returns:
            True if logged successfully, False if duplicate or error
        """
        try:
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            # Calculate days_out (how far ahead is the forecast)
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
            today_dt = datetime.strptime(today_str, "%Y-%m-%d")
            days_out = (target_dt - today_dt).days
            
            if days_out < 0:
                logger.debug(f"[TruthTracker] Skipping past date: {target_date}")
                return False  # Don't log past dates

            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO forecasts 
                (source, run_date, target_date, days_out, pred_high_c, pred_low_c)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (source, today_str, target_date, days_out, high_c, low_c))
            
            self.conn.commit()
            logger.info(f"[TruthTracker] Logged forecast: {source} -> {target_date} "
                       f"(High: {high_c:.1f}°C, Low: {low_c:.1f}°C, days_out={days_out})")
            return True
            
        except Exception as e:
            logger.error(f"[TruthTracker] Failed to log forecast for {source}: {e}")
            return False

    def ingest_actuals(
        self, 
        date_str: str, 
        high_c: float, 
        low_c: float, 
        precip_mm: float = 0.0
    ) -> bool:
        """
        Log observed ground truth data.
        
        Uses INSERT OR REPLACE to update if already exists.
        
        Args:
            date_str: Date of observation (YYYY-MM-DD)
            high_c: Observed high temperature (Celsius)
            low_c: Observed low temperature (Celsius)
            precip_mm: Actual precipitation amount (mm)
            
        Returns:
            True if successful, False on error
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO observations 
                (date, actual_high_c, actual_low_c, actual_precip_mm)
                VALUES (?, ?, ?, ?)
            ''', (date_str, high_c, low_c, precip_mm))
            
            self.conn.commit()
            logger.info(f"[TruthTracker] Logged actuals for {date_str}: "
                       f"Hi {high_c:.1f}°C / Lo {low_c:.1f}°C")
            return True
            
        except Exception as e:
            logger.error(f"[TruthTracker] Failed to ingest actuals: {e}")
            return False

    def get_leaderboard(self, days_out: int = 1) -> List[Tuple]:
        """
        Generate accuracy scores (MAE - Mean Absolute Error).
        
        Args:
            days_out: Filter for specific forecast horizon (1 = Next Day Forecasts)
            
        Returns:
            List of (Source, Count, Avg_High_Error, Avg_Low_Error) tuples
            Sorted by combined error ascending (best first)
        """
        logger.info(f"[TruthTracker] Calculating leaderboard (days_out={days_out})...")
        
        cursor = self.conn.cursor()
        
        # Calculate Mean Absolute Error (MAE) for Highs and Lows
        query = '''
            SELECT 
                f.source,
                COUNT(*) as count,
                ROUND(AVG(ABS(f.pred_high_c - o.actual_high_c)), 2) as high_mae,
                ROUND(AVG(ABS(f.pred_low_c - o.actual_low_c)), 2) as low_mae
            FROM forecasts f
            JOIN observations o ON f.target_date = o.date
            WHERE f.days_out = ?
            GROUP BY f.source
            ORDER BY (AVG(ABS(f.pred_high_c - o.actual_high_c)) + AVG(ABS(f.pred_low_c - o.actual_low_c))) ASC
        '''
        
        results = cursor.execute(query, (days_out,)).fetchall()
        
        logger.info(f"[TruthTracker] Leaderboard results: {len(results)} sources")
        for row in results:
            logger.info(f"[TruthTracker]   {row[0]}: {row[1]} samples, "
                       f"High MAE: {row[2]}°C, Low MAE: {row[3]}°C")
        
        return results

    def get_leaderboard_formatted(self, days_out: int = 1) -> List[LeaderboardEntry]:
        """
        Get formatted leaderboard with ranks and combined MAE.
        
        Args:
            days_out: Filter for specific forecast horizon
            
        Returns:
            List of LeaderboardEntry dictionaries
        """
        raw = self.get_leaderboard(days_out)
        
        entries: List[LeaderboardEntry] = []
        for rank, row in enumerate(raw, 1):
            source, count, high_err, low_err = row
            entries.append({
                "source": source,
                "comparisons": count,
                "high_error_mae": high_err,
                "low_error_mae": low_err,
                "combined_mae": round((high_err + low_err) / 2, 2),
                "rank": rank
            })
        
        return entries

    def get_verification_report(self, days_out: int = 1) -> VerificationReport:
        """
        Generate a detailed verification report.
        
        Args:
            days_out: Filter for specific forecast horizon
            
        Returns:
            VerificationReport with detailed statistics
        """
        logger.info(f"[TruthTracker] Generating verification report (days_out={days_out})...")
        
        cursor = self.conn.cursor()
        
        # Count verified days
        cursor.execute('''
            SELECT COUNT(DISTINCT o.date)
            FROM observations o
            JOIN forecasts f ON f.target_date = o.date
            WHERE f.days_out = ?
        ''', (days_out,))
        verified_days = cursor.fetchone()[0]
        
        # Get per-source stats
        leaderboard = self.get_leaderboard_formatted(days_out)
        
        sources = {}
        for entry in leaderboard:
            sources[entry["source"]] = {
                "comparisons": entry["comparisons"],
                "high_mae": entry["high_error_mae"],
                "low_mae": entry["low_error_mae"],
                "combined_mae": entry["combined_mae"],
                "rank": entry["rank"]
            }
        
        report: VerificationReport = {
            "generated_at": datetime.now().isoformat(),
            "verified_days": verified_days,
            "sources": sources
        }
        
        logger.info(f"[TruthTracker] Report generated: {verified_days} verified days, "
                   f"{len(sources)} sources tracked")
        
        return report

    def get_source_rankings(self, days_out: int = 1, last_n_days: int = 10) -> Dict[str, int]:
        """
        Get source rankings based on accuracy over the last N days.
        
        Returns a dictionary mapping source name to rank (1 = best, 2 = second, 3 = third).
        Sources without enough data get rank 0 (unranked).
        
        Args:
            days_out: Filter for specific forecast horizon (1 = Next Day Forecasts)
            last_n_days: Only consider data from the last N days
            
        Returns:
            Dict mapping source name to rank (1-3, or 0 if unranked)
        """
        logger.info(f"[TruthTracker] Calculating source rankings (last {last_n_days} days)...")
        
        cursor = self.conn.cursor()
        
        # Get date cutoff for last N days
        cutoff_date = (datetime.now() - timedelta(days=last_n_days)).strftime("%Y-%m-%d")
        
        # Calculate Mean Absolute Error (MAE) for last N days only
        query = '''
            SELECT 
                f.source,
                COUNT(*) as count,
                ROUND(AVG(ABS(f.pred_high_c - o.actual_high_c)), 2) as high_mae,
                ROUND(AVG(ABS(f.pred_low_c - o.actual_low_c)), 2) as low_mae
            FROM forecasts f
            JOIN observations o ON f.target_date = o.date
            WHERE f.days_out = ?
              AND o.date >= ?
            GROUP BY f.source
            HAVING COUNT(*) >= 2
            ORDER BY (AVG(ABS(f.pred_high_c - o.actual_high_c)) + AVG(ABS(f.pred_low_c - o.actual_low_c))) ASC
        '''
        
        results = cursor.execute(query, (days_out, cutoff_date)).fetchall()
        
        rankings: Dict[str, int] = {}
        
        # Assign ranks 1-3 based on accuracy (lower MAE = better rank)
        for rank, row in enumerate(results[:3], 1):
            source = row[0]
            rankings[source] = rank
            logger.info(f"[TruthTracker] Rank {rank}: {source} (Hi MAE: {row[2]}°C, Lo MAE: {row[3]}°C)")
        
        # Set unranked sources to 0
        for source in ["Open-Meteo", "NWS", "Met.no"]:
            if source not in rankings:
                rankings[source] = 0
                logger.info(f"[TruthTracker] {source}: Not enough data for ranking")
        
        return rankings

    def generate_leaderboard_markdown(self, days_out: int = 1, last_n_days: int = 10) -> str:
        """
        Generate a markdown file with the current leaderboard.
        
        Args:
            days_out: Filter for specific forecast horizon
            last_n_days: Only consider data from the last N days
            
        Returns:
            Markdown string with leaderboard
        """
        logger.info("[TruthTracker] Generating LEADERBOARD.md content...")
        
        rankings = self.get_source_rankings(days_out, last_n_days)
        leaderboard = self.get_leaderboard(days_out)
        
        # Build markdown
        lines = [
            "# 🏆 Forecast Accuracy Leaderboard",
            "",
            f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')} PST",
            f"**Evaluation Period:** Last {last_n_days} days",
            f"**Forecast Horizon:** Next-Day Predictions",
            "",
            "## Rankings",
            "",
            "| Rank | Source | High Temp Error | Low Temp Error | Samples |",
            "|------|--------|-----------------|----------------|---------|",
        ]
        
        # Sort by rank for display
        ranked_sources = sorted(rankings.items(), key=lambda x: (x[1] == 0, x[1]))
        
        for source, rank in ranked_sources:
            # Find the source in leaderboard data
            source_data = None
            for row in leaderboard:
                if row[0] == source:
                    source_data = row
                    break
            
            if rank == 1:
                rank_display = "🥇 1st"
            elif rank == 2:
                rank_display = "🥈 2nd"
            elif rank == 3:
                rank_display = "🥉 3rd"
            else:
                rank_display = "—"
            
            if source_data:
                _, count, high_err, low_err = source_data
                lines.append(f"| {rank_display} | {source} | ±{high_err}°C | ±{low_err}°C | {count} |")
            else:
                lines.append(f"| {rank_display} | {source} | — | — | 0 |")
        
        lines.extend([
            "",
            "## How Rankings Work",
            "",
            "- Rankings are based on **Mean Absolute Error (MAE)** - lower is better",
            "- Only next-day forecasts from the last 10 days are evaluated",
            "- Sources need at least 2 verified predictions to be ranked",
            "- Ground truth comes from Open-Meteo Archive API (station + reanalysis data)",
            "",
            "---",
            "*Generated by Duck Sun Modesto 🦆☀️*"
        ])
        
        return "\n".join(lines)

    def get_forecast_history(self, source: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent forecast history for a specific source.
        
        Args:
            source: Provider name
            limit: Maximum number of records
            
        Returns:
            List of forecast records
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT target_date, pred_high_c, pred_low_c, days_out, run_date
            FROM forecasts
            WHERE source = ?
            ORDER BY target_date DESC
            LIMIT ?
        ''', (source, limit))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "target_date": row[0],
                "high": row[1],
                "low": row[2],
                "days_out": row[3],
                "run_date": row[4]
            })
        
        return results

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("[TruthTracker] Database connection closed")


async def fetch_and_log_yesterday_actuals(tracker: TruthTracker) -> bool:
    """
    Fetch yesterday's actual weather from Open-Meteo Archive API
    and log it to the database.
    
    This uses the Archive API which provides:
    - Reanalysis data (ERA5 + station data)
    - Reliable ground truth for verification
    - Free and always available (unlike live METAR history)
    
    Args:
        tracker: TruthTracker instance to log data to
        
    Returns:
        True if successful, False on error
    """
    import httpx
    
    logger.info("[TruthTracker] Fetching yesterday's ground truth...")
    
    # Modesto Coordinates
    LAT, LON = 37.6391, -120.9969
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": yesterday,
        "end_date": yesterday,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "America/Los_Angeles"
    }
    
    logger.debug(f"[TruthTracker] Archive API params: {params}")
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            logger.info(f"[TruthTracker] Archive API response: {resp.status_code}")
            
            if resp.status_code != 200:
                logger.warning(f"[TruthTracker] HTTP error: {resp.status_code}")
                return False
            
            data = resp.json()
            
            daily = data.get("daily", {})
            if not daily or not daily.get("time"):
                logger.warning("[TruthTracker] No archive data available yet.")
                return False

            high_c = daily["temperature_2m_max"][0]
            low_c = daily["temperature_2m_min"][0]
            precip_mm = daily.get("precipitation_sum", [0])[0] or 0.0
            
            tracker.ingest_actuals(
                date_str=yesterday,
                high_c=high_c,
                low_c=low_c,
                precip_mm=precip_mm
            )
            
            logger.info(f"[TruthTracker] Ground truth logged: {yesterday} "
                       f"Hi {high_c:.1f}°C / Lo {low_c:.1f}°C / Precip {precip_mm:.1f}mm")
            return True
            
    except Exception as e:
        logger.error(f"[TruthTracker] Failed to fetch historical data: {e}")
        return False


# Legacy function for backwards compatibility with existing code
async def fetch_yesterday_actuals() -> Optional[Dict[str, Any]]:
    """
    Fetch yesterday's actual weather (returns dict instead of logging directly).
    
    Returns:
        Dict with 'date', 'high', 'low', 'precip' or None on error
    """
    import httpx
    
    logger.info("[fetch_yesterday_actuals] Fetching yesterday's actual weather...")
    
    LAT, LON = 37.6391, -120.9969
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": yesterday,
        "end_date": yesterday,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "America/Los_Angeles"
    }
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            daily = data.get("daily", {})
            
            if daily and daily.get("temperature_2m_max"):
                return {
                    "date": yesterday,
                    "high": daily["temperature_2m_max"][0],
                    "low": daily["temperature_2m_min"][0],
                    "precip": daily.get("precipitation_sum", [0])[0] or 0.0
                }
            return None
            
    except Exception as e:
        logger.error(f"[fetch_yesterday_actuals] Error: {e}")
        return None


# Helper for hourly-to-daily conversion (used by tests and legacy code)
def extract_daily_high_low_from_hourly(
    hourly_data: Optional[List[Dict[str, Any]]]
) -> List[DailyHighLow]:
    """
    Convert hourly temperature data to daily high/low summaries.
    
    Note: Prefer using process_daily_high_low() methods on providers directly.
    
    Args:
        hourly_data: List of dicts with 'time' and 'temp_c' keys
        
    Returns:
        List of DailyHighLow dicts with 'date', 'high', 'low'
    """
    if not hourly_data:
        return []
    
    from collections import defaultdict
    daily_temps: Dict[str, List[float]] = defaultdict(list)
    
    for record in hourly_data:
        time_str = record.get("time", "")
        temp = record.get("temp_c")
        
        if not time_str or temp is None:
            continue
        
        try:
            date_str = time_str.split("T")[0] if "T" in time_str else time_str[:10]
            daily_temps[date_str].append(float(temp))
        except (ValueError, IndexError):
            continue
    
    results: List[DailyHighLow] = []
    for date_str in sorted(daily_temps.keys()):
        temps = daily_temps[date_str]
        if temps:
            results.append({
                "date": date_str,
                "high": max(temps),
                "low": min(temps)
            })
    
    return results


# For backwards compatibility
def get_condition_from_weather_code(code: int) -> str:
    """Convert WMO weather code to human-readable condition."""
    conditions = {
        0: "Clear", 1: "Mostly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Fog", 48: "Fog", 51: "Light Drizzle", 53: "Drizzle",
        61: "Light Rain", 63: "Rain", 65: "Heavy Rain",
        80: "Showers", 95: "Thunderstorm"
    }
    return conditions.get(code, "Unknown")


async def run_daily_verification(tracker: TruthTracker) -> Optional[Dict[str, Any]]:
    """Legacy function - use fetch_and_log_yesterday_actuals instead."""
    actuals = await fetch_yesterday_actuals()
    if actuals is None:
        return None
    
    tracker.ingest_actuals(actuals["date"], actuals["high"], actuals["low"], actuals.get("precip", 0))
    leaderboard = tracker.get_leaderboard_formatted()
    
    return {"actuals": actuals, "leaderboard": leaderboard, "verified_date": actuals["date"]}


if __name__ == "__main__":
    """Test the verification system standalone."""
    import asyncio
    
    logging.basicConfig(level=logging.INFO)
    
    tracker = TruthTracker()
    print("Database initialized.")
    
    asyncio.run(fetch_and_log_yesterday_actuals(tracker))
    
    print("\nLeaderboard (Next Day Forecasts):")
    for row in tracker.get_leaderboard(days_out=1):
        print(f"  {row}")
    
    tracker.close()
