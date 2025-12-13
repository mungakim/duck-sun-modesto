"""
Tests for the Truth Tracker Verification System

These tests verify that:
1. The database schema is created correctly
2. Forecasts are logged properly
3. Observations (actuals) are logged properly
4. The leaderboard calculation works correctly
5. The daily high/low extraction from hourly data works

Run with: python -m pytest tests/test_verification.py -v
"""

import pytest
import sqlite3
import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import logging

# Configure test logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Import after setup
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from duck_sun.verification import (
    TruthTracker,
    fetch_yesterday_actuals,
    fetch_and_log_yesterday_actuals,
    extract_daily_high_low_from_hourly,
    DB_PATH
)


class TestTruthTracker:
    """Test suite for the TruthTracker class."""
    
    @pytest.fixture
    def tracker(self, tmp_path):
        """Create a TruthTracker with a temporary database."""
        logger.info(f"[TEST] Creating temporary database in: {tmp_path}")
        test_db = tmp_path / "test_verification.db"
        tracker = TruthTracker(db_path=test_db)
        logger.info(f"[TEST] Tracker initialized with DB: {tracker.db_path}")
        yield tracker
        tracker.close()
        logger.info("[TEST] Tracker closed")
    
    def test_database_schema_creation(self, tracker):
        """Test that the database tables are created correctly."""
        logger.info("[TEST] Testing database schema creation...")
        
        cursor = tracker.conn.cursor()
        
        # Check forecasts table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='forecasts'")
        result = cursor.fetchone()
        logger.info(f"[TEST] Forecasts table check: {result}")
        assert result is not None, "forecasts table should exist"
        
        # Check observations table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='observations'")
        result = cursor.fetchone()
        logger.info(f"[TEST] Observations table check: {result}")
        assert result is not None, "observations table should exist"
        
        # Verify forecasts table schema
        cursor.execute("PRAGMA table_info(forecasts)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        logger.info(f"[TEST] Forecasts table columns: {columns}")
        
        expected_columns = ['id', 'source', 'run_date', 'target_date', 
                          'days_out', 'pred_high_c', 'pred_low_c']
        for col in expected_columns:
            assert col in columns, f"Column {col} should exist in forecasts table"
        
        logger.info("[TEST] Database schema test PASSED")
    
    def test_log_forecast(self, tracker):
        """Test that forecasts are logged correctly."""
        logger.info("[TEST] Testing forecast logging...")
        
        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Log a forecast for Open-Meteo
        logger.info(f"[TEST] Logging Open-Meteo forecast for {tomorrow}")
        tracker.log_forecast(
            source="Open-Meteo",
            target_date=tomorrow,
            high_c=15.5,
            low_c=5.2
        )
        
        # Log a forecast for NWS
        logger.info(f"[TEST] Logging NWS forecast for {tomorrow}")
        tracker.log_forecast(
            source="NWS",
            target_date=tomorrow,
            high_c=14.8,
            low_c=6.0
        )
        
        # Verify forecasts were stored
        cursor = tracker.conn.cursor()
        cursor.execute("SELECT * FROM forecasts WHERE target_date = ?", (tomorrow,))
        results = cursor.fetchall()
        
        logger.info(f"[TEST] Found {len(results)} forecast records")
        for r in results:
            logger.info(f"[TEST]   Row: {r}")
        
        assert len(results) == 2, "Should have 2 forecast records"
        
        # Check values
        cursor.execute("SELECT source, pred_high_c, pred_low_c FROM forecasts WHERE source = 'Open-Meteo'")
        om_row = cursor.fetchone()
        logger.info(f"[TEST] Open-Meteo row: {om_row}")
        assert om_row[1] == 15.5, "High temp should be 15.5"
        assert om_row[2] == 5.2, "Low temp should be 5.2"
        
        logger.info("[TEST] Forecast logging test PASSED")
    
    def test_log_forecast_skip_past_dates(self, tracker):
        """Test that past dates are not logged."""
        logger.info("[TEST] Testing past date skipping...")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Should return False for past dates
        result = tracker.log_forecast("Open-Meteo", yesterday, 15.0, 5.0)
        assert result == False, "Should return False for past dates"
        
        cursor = tracker.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM forecasts WHERE target_date = ?", (yesterday,))
        count = cursor.fetchone()[0]
        
        assert count == 0, "Should have no forecasts for past dates"
        
        logger.info("[TEST] Past date skipping test PASSED")
    
    def test_ingest_actuals(self, tracker):
        """Test that actuals/observations are logged correctly."""
        logger.info("[TEST] Testing actuals ingestion...")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        tracker.ingest_actuals(
            date_str=yesterday,
            high_c=16.2,
            low_c=4.8,
            precip_mm=0.0
        )
        
        cursor = tracker.conn.cursor()
        cursor.execute("SELECT * FROM observations WHERE date = ?", (yesterday,))
        result = cursor.fetchone()
        
        logger.info(f"[TEST] Observations row: {result}")
        
        assert result is not None, "Should have observation record"
        assert result[1] == 16.2, "High should be 16.2"
        assert result[2] == 4.8, "Low should be 4.8"
        
        logger.info("[TEST] Actuals ingestion test PASSED")
    
    def test_ingest_actuals_upsert(self, tracker):
        """Test that actuals can be updated (upserted)."""
        logger.info("[TEST] Testing actuals upsert...")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # First insert
        tracker.ingest_actuals(yesterday, 15.0, 5.0, 0.0)
        
        # Upsert with new values
        tracker.ingest_actuals(yesterday, 16.0, 4.0, 1.5)
        
        cursor = tracker.conn.cursor()
        cursor.execute("SELECT actual_high_c, actual_low_c, actual_precip_mm FROM observations WHERE date = ?", (yesterday,))
        result = cursor.fetchone()
        
        logger.info(f"[TEST] Upsert result: {result}")
        
        assert result[0] == 16.0, "High should be updated to 16.0"
        assert result[1] == 4.0, "Low should be updated to 4.0"
        assert result[2] == 1.5, "Precip should be updated to 1.5"
        
        logger.info("[TEST] Actuals upsert test PASSED")
    
    def test_calculate_days_out(self, tracker):
        """Test that days_out is calculated correctly."""
        logger.info("[TEST] Testing days_out calculation...")
        
        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        day_after = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
        
        tracker.log_forecast("Test", today, 15.0, 5.0)
        tracker.log_forecast("Test", tomorrow, 16.0, 6.0)
        tracker.log_forecast("Test", day_after, 17.0, 7.0)
        
        cursor = tracker.conn.cursor()
        cursor.execute("SELECT target_date, days_out FROM forecasts WHERE source = 'Test' ORDER BY target_date")
        results = cursor.fetchall()
        
        logger.info(f"[TEST] Days out results: {results}")
        
        assert results[0][1] == 0, "Today should be 0 days out"
        assert results[1][1] == 1, "Tomorrow should be 1 day out"
        assert results[2][1] == 2, "Day after should be 2 days out"
        
        logger.info("[TEST] Days out calculation test PASSED")
    
    def test_get_leaderboard_empty(self, tracker):
        """Test leaderboard when no data exists."""
        logger.info("[TEST] Testing empty leaderboard...")
        
        results = tracker.get_leaderboard()
        logger.info(f"[TEST] Empty leaderboard: {results}")
        
        assert len(results) == 0, "Should return empty list when no verified data"
        
        logger.info("[TEST] Empty leaderboard test PASSED")
    
    def test_get_leaderboard_with_data(self, tracker):
        """Test leaderboard calculation with sample data."""
        logger.info("[TEST] Testing leaderboard with data...")
        
        # Set up test data: forecasts for yesterday that we can verify
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        
        # Insert forecasts directly with correct days_out
        cursor = tracker.conn.cursor()
        
        # Simulated: forecasts made 2 days ago predicting yesterday (days_out=1)
        # Open-Meteo predicted 15C high, 5C low
        cursor.execute('''
            INSERT INTO forecasts (source, run_date, target_date, days_out, pred_high_c, pred_low_c)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ("Open-Meteo", two_days_ago, yesterday, 1, 15.0, 5.0))
        
        # NWS predicted 14C high, 6C low
        cursor.execute('''
            INSERT INTO forecasts (source, run_date, target_date, days_out, pred_high_c, pred_low_c)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ("NWS", two_days_ago, yesterday, 1, 14.0, 6.0))
        
        # Met.no predicted 16C high, 4C low
        cursor.execute('''
            INSERT INTO forecasts (source, run_date, target_date, days_out, pred_high_c, pred_low_c)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ("Met.no", two_days_ago, yesterday, 1, 16.0, 4.0))
        
        tracker.conn.commit()
        
        # Actual for yesterday: 14.5C high, 5.5C low
        tracker.ingest_actuals(yesterday, 14.5, 5.5, 0.0)
        
        # Get leaderboard
        leaderboard = tracker.get_leaderboard(days_out=1)
        logger.info(f"[TEST] Leaderboard results: {leaderboard}")
        
        assert len(leaderboard) == 3, "Should have 3 sources in leaderboard"
        
        # Convert to dict for easier checking
        scores = {row[0]: {"count": row[1], "high_err": row[2], "low_err": row[3]} 
                  for row in leaderboard}
        
        logger.info(f"[TEST] Scores dict: {scores}")
        
        # Check errors (absolute)
        # Open-Meteo: |15.0 - 14.5| = 0.5 high, |5.0 - 5.5| = 0.5 low
        # NWS: |14.0 - 14.5| = 0.5 high, |6.0 - 5.5| = 0.5 low
        # Met.no: |16.0 - 14.5| = 1.5 high, |4.0 - 5.5| = 1.5 low
        
        assert abs(scores["Open-Meteo"]["high_err"] - 0.5) < 0.01, f"Open-Meteo high error should be ~0.5, got {scores['Open-Meteo']['high_err']}"
        assert abs(scores["NWS"]["high_err"] - 0.5) < 0.01, f"NWS high error should be ~0.5, got {scores['NWS']['high_err']}"
        assert abs(scores["Met.no"]["high_err"] - 1.5) < 0.01, f"Met.no high error should be ~1.5, got {scores['Met.no']['high_err']}"
        
        logger.info("[TEST] Leaderboard calculation test PASSED")
    
    def test_get_verification_report(self, tracker):
        """Test the detailed verification report generation."""
        logger.info("[TEST] Testing verification report...")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        
        cursor = tracker.conn.cursor()
        
        # Add forecasts
        cursor.execute('''
            INSERT INTO forecasts (source, run_date, target_date, days_out, pred_high_c, pred_low_c)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ("Open-Meteo", two_days_ago, yesterday, 1, 15.0, 5.0))
        
        tracker.conn.commit()
        
        # Add actuals
        tracker.ingest_actuals(yesterday, 14.5, 5.5, 0.0)
        
        # Get report
        report = tracker.get_verification_report(days_out=1)
        logger.info(f"[TEST] Verification report: {report}")
        
        assert "verified_days" in report
        assert "sources" in report
        
        logger.info("[TEST] Verification report test PASSED")


class TestExtractDailyHighLow:
    """Test suite for the hourly-to-daily conversion helper."""
    
    def test_extract_from_hourly_list(self):
        """Test extracting daily high/low from hourly temperature list."""
        logger.info("[TEST] Testing hourly to daily extraction...")
        
        # Mock hourly data for NWS/Met.no format
        hourly_data = [
            {"time": "2025-12-12T00:00:00", "temp_c": 5.0},
            {"time": "2025-12-12T06:00:00", "temp_c": 4.0},  # Low
            {"time": "2025-12-12T12:00:00", "temp_c": 15.0}, # High
            {"time": "2025-12-12T18:00:00", "temp_c": 12.0},
            {"time": "2025-12-13T00:00:00", "temp_c": 6.0},
            {"time": "2025-12-13T06:00:00", "temp_c": 5.0},  # Low
            {"time": "2025-12-13T12:00:00", "temp_c": 14.0}, # High
            {"time": "2025-12-13T18:00:00", "temp_c": 10.0},
        ]
        
        result = extract_daily_high_low_from_hourly(hourly_data)
        logger.info(f"[TEST] Extraction result: {result}")
        
        assert len(result) == 2, "Should have 2 days"
        assert result[0]["date"] == "2025-12-12"
        assert result[0]["high"] == 15.0
        assert result[0]["low"] == 4.0
        assert result[1]["date"] == "2025-12-13"
        assert result[1]["high"] == 14.0
        assert result[1]["low"] == 5.0
        
        logger.info("[TEST] Hourly to daily extraction test PASSED")
    
    def test_extract_empty_list(self):
        """Test extraction with empty data."""
        logger.info("[TEST] Testing empty list extraction...")
        
        result = extract_daily_high_low_from_hourly([])
        logger.info(f"[TEST] Empty result: {result}")
        
        assert result == [], "Should return empty list"
        
        logger.info("[TEST] Empty list extraction test PASSED")
    
    def test_extract_none_handling(self):
        """Test extraction handles None input."""
        logger.info("[TEST] Testing None handling...")
        
        result = extract_daily_high_low_from_hourly(None)
        logger.info(f"[TEST] None result: {result}")
        
        assert result == [], "Should return empty list for None input"
        
        logger.info("[TEST] None handling test PASSED")


class TestFetchYesterdayActuals:
    """Test suite for fetching yesterday's actual weather."""
    
    @pytest.mark.asyncio
    async def test_fetch_yesterday_actuals(self):
        """Test fetching actual weather data from Open-Meteo historical API."""
        logger.info("[TEST] Testing fetch_yesterday_actuals (live API call)...")
        
        result = await fetch_yesterday_actuals()
        logger.info(f"[TEST] Yesterday's actuals: {result}")
        
        if result is not None:  # API might fail in CI
            assert "date" in result
            assert "high" in result
            assert "low" in result
            logger.info(f"[TEST]   Date: {result['date']}")
            logger.info(f"[TEST]   High: {result['high']}°C")
            logger.info(f"[TEST]   Low: {result['low']}°C")
        else:
            logger.warning("[TEST] API call returned None (network issue?)")
        
        logger.info("[TEST] Fetch yesterday actuals test PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
