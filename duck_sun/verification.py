"""
The Truth Tracker - Verification System for Duck Sun Modesto

This module tracks forecast accuracy by logging predictions and comparing them
against observed weather data. It answers the question: "Who is Most Accurate?"

Architecture:
1. THE VAULT (Database): SQLite file storing all forecasts and actuals
2. THE LOGGER (Input): Saves forecasts from each source with timestamps
3. THE AUDITOR (Output): Grades each source against ground truth

Ground Truth Source:
- Uses Open-Meteo's Historical Weather API which combines:
  - Station data (KMOD Modesto Airport)
  - Reanalysis data (satellite + models)
  - This ensures we never have "missing" verification data

Usage:
    from duck_sun.verification import TruthTracker, fetch_yesterday_actuals

    tracker = TruthTracker()
    tracker.log_forecast("Open-Meteo", "2025-12-13", high=15.0, low=5.0, precip=10)
    
    actuals = await fetch_yesterday_actuals()
    tracker.ingest_actuals(actuals['date'], actuals['high'], actuals['low'], "Clear")
    
    leaderboard = tracker.get_leaderboard()
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, TypedDict
from collections import defaultdict

logger = logging.getLogger(__name__)

# Default database path (at project root)
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
    The Truth Tracker - Forecast Verification System.
    
    Manages a SQLite database that stores:
    1. Forecasts: What each source predicted (with timestamp)
    2. Observations: What actually happened (ground truth)
    
    The tracker calculates Mean Absolute Error (MAE) for each source
    to determine which provider is most accurate.
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
        """Create tables if they don't exist, with auto-migration for schema changes."""
        logger.debug("[TruthTracker] Creating database schema if needed...")
        
        cursor = self.conn.cursor()
        
        # Check if we need to migrate (old schema detection)
        needs_migration = False
        try:
            # Check if forecasts table has run_timestamp column
            cursor.execute("PRAGMA table_info(forecasts)")
            columns = [row[1] for row in cursor.fetchall()]
            if columns and 'run_timestamp' not in columns:
                logger.warning("[TruthTracker] Old schema detected (missing run_timestamp) - migrating...")
                needs_migration = True
            
            # Check if observations table has actual_high column
            cursor.execute("PRAGMA table_info(observations)")
            columns = [row[1] for row in cursor.fetchall()]
            if columns and 'actual_high' not in columns:
                logger.warning("[TruthTracker] Old schema detected (missing actual_high) - migrating...")
                needs_migration = True
        except Exception as e:
            logger.debug(f"[TruthTracker] Schema check: {e}")
        
        # Drop old tables if migration needed
        if needs_migration:
            logger.info("[TruthTracker] Dropping old tables for schema migration...")
            cursor.execute("DROP TABLE IF EXISTS forecasts")
            cursor.execute("DROP TABLE IF EXISTS observations")
            self.conn.commit()
            logger.info("[TruthTracker] Old tables dropped - recreating with new schema")
        
        # Table 1: The Forecasts (What they guessed)
        # UNIQUE constraint prevents duplicate entries for same source/time/target
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,              -- 'Open-Meteo', 'NWS', 'Met.no'
                run_timestamp TEXT NOT NULL,       -- When the script ran (YYYY-MM-DD_HH)
                target_date TEXT NOT NULL,         -- The date being forecast (YYYY-MM-DD)
                days_out INTEGER NOT NULL,         -- 0 = Today, 1 = Tomorrow, etc.
                temp_high REAL,                    -- Predicted high (Celsius)
                temp_low REAL,                     -- Predicted low (Celsius)
                precip_prob INTEGER DEFAULT 0,    -- Precipitation probability (%)
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, run_timestamp, target_date)
            )
        ''')
        
        # Table 2: The Actuals/Observations (What happened)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                date TEXT PRIMARY KEY,            -- YYYY-MM-DD
                actual_high REAL,                 -- Observed high (Celsius)
                actual_low REAL,                  -- Observed low (Celsius)
                observed_weather TEXT,            -- 'Clear', 'Fog', 'Rain', etc.
                precip_mm REAL DEFAULT 0,         -- Actual precipitation (mm)
                source TEXT DEFAULT 'Open-Meteo Historical',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
        high: float, 
        low: float, 
        precip: int = 0
    ) -> bool:
        """
        Save a single forecast entry to the database.
        
        Args:
            source: Provider name ('Open-Meteo', 'NWS', 'Met.no')
            target_date: Date being forecast (YYYY-MM-DD)
            high: Predicted high temperature (Celsius)
            low: Predicted low temperature (Celsius)
            precip: Precipitation probability (%)
            
        Returns:
            True if logged successfully, False if duplicate or error
        """
        # Generate run timestamp (grouped by hour to avoid excessive duplicates)
        run_ts = datetime.now().strftime("%Y-%m-%d_%H")
        
        # Calculate 'days_out' (how far ahead is the forecast)
        try:
            target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
            today = datetime.now().date()
            days_out = (target_dt - today).days
            logger.debug(f"[TruthTracker] Calculated days_out={days_out} for target={target_date}")
        except ValueError as e:
            logger.error(f"[TruthTracker] Invalid target_date format: {target_date}")
            days_out = -1

        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO forecasts 
                (source, run_timestamp, target_date, days_out, temp_high, temp_low, precip_prob)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (source, run_ts, target_date, days_out, high, low, precip))
            
            rows_affected = cursor.rowcount
            self.conn.commit()
            
            if rows_affected > 0:
                logger.info(f"[TruthTracker] Logged forecast: {source} -> {target_date} "
                           f"(High: {high}°C, Low: {low}°C, days_out={days_out})")
                return True
            else:
                logger.debug(f"[TruthTracker] Duplicate forecast ignored: {source} -> {target_date}")
                return False
                
        except Exception as e:
            logger.error(f"[TruthTracker] Failed to log forecast: {e}")
            return False

    def ingest_actuals(
        self, 
        date_str: str, 
        high: float, 
        low: float, 
        condition: str,
        precip_mm: float = 0.0
    ) -> bool:
        """
        Log the ground truth observation for a specific date.
        
        Uses INSERT OR REPLACE to update if already exists.
        
        Args:
            date_str: Date of observation (YYYY-MM-DD)
            high: Observed high temperature (Celsius)
            low: Observed low temperature (Celsius)
            condition: Weather condition description
            precip_mm: Actual precipitation amount (mm)
            
        Returns:
            True if successful, False on error
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO observations 
                (date, actual_high, actual_low, observed_weather, precip_mm)
                VALUES (?, ?, ?, ?, ?)
            ''', (date_str, high, low, condition, precip_mm))
            
            self.conn.commit()
            logger.info(f"[TruthTracker] Logged actuals for {date_str}: "
                       f"High {high}°C, Low {low}°C, Condition: {condition}")
            return True
            
        except Exception as e:
            logger.error(f"[TruthTracker] Failed to log actuals: {e}")
            return False

    def get_leaderboard(self, days_back: int = 30) -> List[tuple]:
        """
        Calculate Mean Absolute Error (MAE) for each source.
        
        Focuses on "Next Day" (days_out=1) forecasts for fair comparison.
        
        Args:
            days_back: How many days of history to consider
            
        Returns:
            List of tuples: (source, count, high_error, low_error)
            Sorted by high_error ascending (best first)
        """
        logger.info(f"[TruthTracker] Calculating leaderboard (last {days_back} days)...")
        
        cursor = self.conn.cursor()
        
        # SQL magic: Join forecasts with actuals and calculate MAE
        # Only consider days_out = 1 (Next Day forecasts) for apples-to-apples comparison
        query = '''
            SELECT 
                f.source,
                COUNT(*) as comparisons,
                ROUND(AVG(ABS(f.temp_high - o.actual_high)), 2) as high_error,
                ROUND(AVG(ABS(f.temp_low - o.actual_low)), 2) as low_error
            FROM forecasts f
            JOIN observations o ON f.target_date = o.date
            WHERE f.days_out = 1
              AND o.date >= date('now', ? || ' days')
            GROUP BY f.source
            ORDER BY high_error ASC
        '''
        
        results = cursor.execute(query, (f"-{days_back}",)).fetchall()
        
        logger.info(f"[TruthTracker] Leaderboard results: {len(results)} sources")
        for row in results:
            logger.info(f"[TruthTracker]   {row[0]}: {row[1]} comparisons, "
                       f"High MAE: {row[2]}°C, Low MAE: {row[3]}°C")
        
        return results

    def get_leaderboard_formatted(self, days_back: int = 30) -> List[LeaderboardEntry]:
        """
        Get formatted leaderboard with ranks and combined MAE.
        
        Args:
            days_back: How many days of history to consider
            
        Returns:
            List of LeaderboardEntry dictionaries
        """
        raw = self.get_leaderboard(days_back)
        
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

    def get_verification_report(self, days_back: int = 30) -> VerificationReport:
        """
        Generate a detailed verification report.
        
        Args:
            days_back: How many days of history to consider
            
        Returns:
            VerificationReport with detailed statistics
        """
        logger.info(f"[TruthTracker] Generating verification report (last {days_back} days)...")
        
        cursor = self.conn.cursor()
        
        # Count verified days
        cursor.execute('''
            SELECT COUNT(DISTINCT o.date)
            FROM observations o
            JOIN forecasts f ON f.target_date = o.date
            WHERE o.date >= date('now', ? || ' days')
        ''', (f"-{days_back}",))
        verified_days = cursor.fetchone()[0]
        
        # Get per-source stats
        leaderboard = self.get_leaderboard_formatted(days_back)
        
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
            SELECT target_date, temp_high, temp_low, days_out, run_timestamp
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
                "run_timestamp": row[4]
            })
        
        return results

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("[TruthTracker] Database connection closed")


def extract_daily_high_low_from_hourly(
    hourly_data: Optional[List[Dict[str, Any]]]
) -> List[DailyHighLow]:
    """
    Convert hourly temperature data to daily high/low summaries.
    
    This is needed because NWS and Met.no return hourly temps, but we need
    daily high/low for the verification system.
    
    Args:
        hourly_data: List of dicts with 'time' and 'temp_c' keys
                    Time format: ISO (2025-12-12T06:00:00)
        
    Returns:
        List of DailyHighLow dicts with 'date', 'high', 'low'
    """
    if not hourly_data:
        logger.debug("[extract_daily_high_low] No data provided, returning empty list")
        return []
    
    logger.debug(f"[extract_daily_high_low] Processing {len(hourly_data)} hourly records")
    
    # Group by date
    daily_temps: Dict[str, List[float]] = defaultdict(list)
    
    for record in hourly_data:
        time_str = record.get("time", "")
        temp = record.get("temp_c")
        
        if not time_str or temp is None:
            continue
        
        # Extract date from ISO format
        try:
            # Handle various ISO formats
            if "T" in time_str:
                date_str = time_str.split("T")[0]
            else:
                date_str = time_str[:10]
            
            daily_temps[date_str].append(float(temp))
        except (ValueError, IndexError) as e:
            logger.warning(f"[extract_daily_high_low] Failed to parse: {time_str}: {e}")
            continue
    
    # Calculate high/low for each date
    results: List[DailyHighLow] = []
    
    for date_str in sorted(daily_temps.keys()):
        temps = daily_temps[date_str]
        if temps:
            result: DailyHighLow = {
                "date": date_str,
                "high": max(temps),
                "low": min(temps)
            }
            results.append(result)
            logger.debug(f"[extract_daily_high_low] {date_str}: "
                        f"High={result['high']:.1f}°C, Low={result['low']:.1f}°C")
    
    logger.info(f"[extract_daily_high_low] Extracted {len(results)} daily summaries")
    return results


async def fetch_yesterday_actuals() -> Optional[Dict[str, Any]]:
    """
    Fetch yesterday's actual weather from Open-Meteo Historical API.
    
    This uses the Archive API which provides:
    - Reanalysis data (ERA5 + stations)
    - Reliable ground truth for verification
    - Free and always available (unlike live METAR history)
    
    Returns:
        Dict with 'date', 'high', 'low', 'precip' or None on error
    """
    import httpx
    
    logger.info("[fetch_yesterday_actuals] Fetching yesterday's actual weather...")
    
    # Modesto Coordinates
    URL = "https://archive-api.open-meteo.com/v1/archive"
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    params = {
        "latitude": 37.6391,
        "longitude": -120.9969,
        "start_date": yesterday,
        "end_date": yesterday,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code"],
        "timezone": "America/Los_Angeles"
    }
    
    logger.debug(f"[fetch_yesterday_actuals] Request params: {params}")
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(URL, params=params)
            logger.info(f"[fetch_yesterday_actuals] Response status: {resp.status_code}")
            
            if resp.status_code != 200:
                logger.warning(f"[fetch_yesterday_actuals] HTTP error: {resp.status_code}")
                return None
            
            data = resp.json()
            logger.debug(f"[fetch_yesterday_actuals] Response data: {data}")
            
            daily = data.get("daily", {})
            if daily and daily.get("temperature_2m_max"):
                result = {
                    "date": yesterday,
                    "high": daily["temperature_2m_max"][0],
                    "low": daily["temperature_2m_min"][0],
                    "precip": daily.get("precipitation_sum", [0])[0],
                    "weather_code": daily.get("weather_code", [0])[0]
                }
                
                logger.info(f"[fetch_yesterday_actuals] Successfully fetched: "
                           f"Date={yesterday}, High={result['high']}°C, "
                           f"Low={result['low']}°C, Precip={result['precip']}mm")
                
                return result
            else:
                logger.warning("[fetch_yesterday_actuals] No daily data in response")
                return None
                
    except httpx.TimeoutException:
        logger.warning("[fetch_yesterday_actuals] Request timed out")
        return None
    except Exception as e:
        logger.error(f"[fetch_yesterday_actuals] Error: {e}", exc_info=True)
        return None


def get_condition_from_weather_code(code: int) -> str:
    """
    Convert WMO weather code to human-readable condition.
    
    Args:
        code: WMO weather code (0-99)
        
    Returns:
        Human-readable condition string
    """
    # Reference: https://open-meteo.com/en/docs
    conditions = {
        0: "Clear",
        1: "Mostly Clear",
        2: "Partly Cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Fog",
        51: "Light Drizzle",
        53: "Drizzle",
        55: "Heavy Drizzle",
        61: "Light Rain",
        63: "Rain",
        65: "Heavy Rain",
        71: "Light Snow",
        73: "Snow",
        75: "Heavy Snow",
        80: "Showers",
        81: "Showers",
        82: "Heavy Showers",
        95: "Thunderstorm",
    }
    return conditions.get(code, "Unknown")


# Convenience function for main.py integration
async def run_daily_verification(tracker: TruthTracker) -> Optional[Dict[str, Any]]:
    """
    Run the daily verification process.
    
    This function:
    1. Fetches yesterday's actuals
    2. Logs them to the database
    3. Returns the leaderboard
    
    Args:
        tracker: TruthTracker instance
        
    Returns:
        Dict with 'actuals' and 'leaderboard' or None on error
    """
    logger.info("[run_daily_verification] Starting daily verification...")
    
    # Fetch yesterday's actuals
    actuals = await fetch_yesterday_actuals()
    
    if actuals is None:
        logger.warning("[run_daily_verification] Could not fetch actuals")
        return None
    
    # Ingest into database
    condition = get_condition_from_weather_code(actuals.get("weather_code", 0))
    tracker.ingest_actuals(
        date_str=actuals["date"],
        high=actuals["high"],
        low=actuals["low"],
        condition=condition,
        precip_mm=actuals.get("precip", 0)
    )
    
    # Get leaderboard
    leaderboard = tracker.get_leaderboard_formatted()
    
    result = {
        "actuals": actuals,
        "leaderboard": leaderboard,
        "verified_date": actuals["date"]
    }
    
    logger.info(f"[run_daily_verification] Verification complete for {actuals['date']}")
    
    return result


if __name__ == "__main__":
    """Test the verification system standalone."""
    import asyncio
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        print("=" * 60)
        print("Testing Truth Tracker Verification System")
        print("=" * 60)
        
        # Create tracker (uses default verification.db)
        tracker = TruthTracker()
        
        # Test logging forecasts
        print("\n1. Logging sample forecasts...")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        tracker.log_forecast("Open-Meteo", tomorrow, high=15.5, low=5.2, precip=10)
        tracker.log_forecast("NWS", tomorrow, high=14.8, low=6.0, precip=15)
        tracker.log_forecast("Met.no", tomorrow, high=16.0, low=4.5, precip=5)
        
        # Test fetching actuals
        print("\n2. Fetching yesterday's actuals...")
        actuals = await fetch_yesterday_actuals()
        if actuals:
            print(f"   Date: {actuals['date']}")
            print(f"   High: {actuals['high']}°C")
            print(f"   Low: {actuals['low']}°C")
            print(f"   Precip: {actuals['precip']}mm")
            
            condition = get_condition_from_weather_code(actuals.get("weather_code", 0))
            tracker.ingest_actuals(
                actuals["date"], 
                actuals["high"], 
                actuals["low"], 
                condition,
                actuals.get("precip", 0)
            )
        else:
            print("   Could not fetch actuals (API error)")
        
        # Show leaderboard
        print("\n3. Accuracy Leaderboard:")
        leaderboard = tracker.get_leaderboard()
        if leaderboard:
            print(f"{'Source':<15} {'Count':<8} {'High MAE':<12} {'Low MAE':<12}")
            print("-" * 50)
            for row in leaderboard:
                print(f"{row[0]:<15} {row[1]:<8} {row[2]:>8.2f}°C   {row[3]:>8.2f}°C")
        else:
            print("   No verified data yet (need 24+ hours of data)")
        
        # Show verification report
        print("\n4. Verification Report:")
        report = tracker.get_verification_report()
        print(f"   Verified Days: {report['verified_days']}")
        print(f"   Sources Tracked: {len(report['sources'])}")
        
        tracker.close()
        print("\n" + "=" * 60)
        print("Test complete!")
    
    asyncio.run(test())