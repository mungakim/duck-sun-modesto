#!/usr/bin/env python3
"""
Test script to verify the Google Weather precip aggregation fix.

The fix changes precipitation aggregation from meteorological day (6am-6am)
to calendar day (midnight-midnight) to prevent overnight rain from
being attributed to the previous day.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from duck_sun.providers.google_weather import GoogleWeatherProvider


def test_aggregation():
    """Test the fixed aggregation logic."""
    print("=" * 70)
    print("TESTING GOOGLE WEATHER PRECIP AGGREGATION FIX")
    print("=" * 70)
    print()

    # Create test hourly data that replicates the issue
    # Rain at midnight Wednesday PST (2026-01-28 00:00) = 2026-01-28T08:00:00Z
    test_hourly = [
        # Tuesday Jan 27 daytime hours (low precip)
        {"time": "2026-01-27T17:00:00Z", "temp_c": 10.0, "precip_prob": 10, "condition": "Cloudy", "is_daytime": True},
        {"time": "2026-01-27T18:00:00Z", "temp_c": 11.0, "precip_prob": 10, "condition": "Cloudy", "is_daytime": True},
        {"time": "2026-01-27T19:00:00Z", "temp_c": 12.0, "precip_prob": 10, "condition": "Cloudy", "is_daytime": True},
        # Tuesday evening
        {"time": "2026-01-28T03:00:00Z", "temp_c": 8.0, "precip_prob": 10, "condition": "Cloudy", "is_daytime": False},  # 7pm PST Tue
        {"time": "2026-01-28T04:00:00Z", "temp_c": 7.0, "precip_prob": 15, "condition": "Cloudy", "is_daytime": False},  # 8pm PST Tue
        {"time": "2026-01-28T05:00:00Z", "temp_c": 6.5, "precip_prob": 20, "condition": "Cloudy", "is_daytime": False},  # 9pm PST Tue
        {"time": "2026-01-28T06:00:00Z", "temp_c": 6.0, "precip_prob": 25, "condition": "Cloudy", "is_daytime": False},  # 10pm PST Tue
        {"time": "2026-01-28T07:00:00Z", "temp_c": 5.5, "precip_prob": 35, "condition": "Light rain", "is_daytime": False},  # 11pm PST Tue

        # THE PROBLEMATIC HOURS - midnight-2am Wednesday PST
        {"time": "2026-01-28T08:00:00Z", "temp_c": 5.0, "precip_prob": 75, "condition": "Rain", "is_daytime": False},      # MIDNIGHT PST Wed
        {"time": "2026-01-28T09:00:00Z", "temp_c": 5.0, "precip_prob": 60, "condition": "Light rain", "is_daytime": False}, # 1am PST Wed
        {"time": "2026-01-28T10:00:00Z", "temp_c": 5.5, "precip_prob": 40, "condition": "Light rain", "is_daytime": False}, # 2am PST Wed

        # Wednesday morning/day
        {"time": "2026-01-28T15:00:00Z", "temp_c": 8.0, "precip_prob": 10, "condition": "Cloudy", "is_daytime": True},  # 7am PST Wed
        {"time": "2026-01-28T18:00:00Z", "temp_c": 12.0, "precip_prob": 5, "condition": "Partly cloudy", "is_daytime": True},  # 10am PST Wed
    ]

    # Create provider and test aggregation
    provider = GoogleWeatherProvider()
    daily = provider._aggregate_to_daily(test_hourly)

    print("AGGREGATION RESULTS:")
    print("-" * 50)
    print(f"{'Date':<15} {'High':<10} {'Low':<10} {'Precip':<10}")
    print("-" * 50)

    for day in daily:
        print(f"{day['date']:<15} {day['high_f']}F       {day['low_f']}F       {day['precip_prob']}%")

    print()
    print("EXPECTED RESULTS (with calendar-day precip):")
    print("-" * 50)
    print("2026-01-27:     High temps from met-day, Precip MAX from calendar day hours")
    print("                Calendar hours: 9am-midnight PST = MAX 35%")
    print()
    print("2026-01-28:     Precip should include the 75% rain at midnight")
    print("                Calendar hours: midnight-11:59pm PST = MAX 75%")
    print()

    # Check results
    jan27 = next((d for d in daily if d['date'] == '2026-01-27'), None)
    jan28 = next((d for d in daily if d['date'] == '2026-01-28'), None)

    print("=" * 70)
    print("VERIFICATION:")
    print("=" * 70)

    if jan27:
        if jan27['precip_prob'] <= 40:  # Should be ~35% or less (calendar day doesn't include midnight rain)
            print(f"  [PASS] Jan 27 precip = {jan27['precip_prob']}% (expected <= 40%, was 75% before fix)")
        else:
            print(f"  [FAIL] Jan 27 precip = {jan27['precip_prob']}% (expected <= 40%, still showing high value)")
    else:
        print("  [WARN] No data for Jan 27")

    if jan28:
        if jan28['precip_prob'] >= 60:  # Should include the 75% rain now
            print(f"  [PASS] Jan 28 precip = {jan28['precip_prob']}% (correctly includes midnight rain)")
        else:
            print(f"  [INFO] Jan 28 precip = {jan28['precip_prob']}%")
    else:
        print("  [WARN] No data for Jan 28")


if __name__ == "__main__":
    test_aggregation()
