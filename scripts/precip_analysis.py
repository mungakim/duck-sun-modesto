#!/usr/bin/env python3
"""
Precipitation Analysis Script for Duck Sun Modesto

Investigates the 75% vs 1-15% discrepancy between Google Weather
and other sources for Tuesday 2026-01-27.

ROOT CAUSE IDENTIFIED:
- Google Weather uses MAX hourly precip for daily aggregation
- Meteorological day (6am-6am) attributes early morning hours to previous day
- A 75% rain event at midnight Wednesday PST (2026-01-28T08:00Z)
  gets counted as Tuesday's precip

This script:
1. Loads cached data from all providers
2. Shows the hourly breakdown for the problematic day
3. Compares calendar-day vs meteorological-day aggregation
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

CACHE_DIR = Path("outputs/cache")
TZ = ZoneInfo("America/Los_Angeles")


def load_google_weather():
    """Load Google Weather cache data."""
    path = CACHE_DIR / "google_weather_lkg.json"
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return data.get('data', data)


def analyze_tuesday_precip():
    """Analyze the precip discrepancy for Tuesday 2026-01-27."""
    print("=" * 70)
    print("PRECIPITATION ANALYSIS - Tuesday 2026-01-27 Discrepancy")
    print("=" * 70)
    print()

    google = load_google_weather()
    if not google:
        print("ERROR: No Google Weather cache found")
        return

    # Show current daily aggregation
    print("CURRENT DAILY VALUES (Meteorological Day, MAX aggregation):")
    print("-" * 50)
    for day in google.get('daily', []):
        date = day.get('date', '')
        precip = day.get('precip_prob', 0)
        if '2026-01-2' in date:  # Jan 26-29
            print(f"  {date}: {precip}%")

    print()
    print("HOURLY BREAKDOWN (showing hours with >0% precip):")
    print("-" * 50)

    # Group hourly by calendar day AND meteorological day
    calendar_day_precip = {}  # date -> list of precip values
    met_day_precip = {}       # date -> list of precip values

    for h in google.get('hourly', []):
        time_str = h.get('time', '')
        precip = h.get('precip_prob', 0)

        if not time_str:
            continue

        # Parse time
        if 'Z' in time_str:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00')).astimezone(TZ)
        else:
            dt = datetime.fromisoformat(time_str).astimezone(TZ)

        # Calendar day
        cal_date = dt.strftime('%Y-%m-%d')
        if cal_date not in calendar_day_precip:
            calendar_day_precip[cal_date] = []
        calendar_day_precip[cal_date].append(precip)

        # Meteorological day (6am-6am)
        if dt.hour < 6:
            met_day = dt - timedelta(days=1)
        else:
            met_day = dt
        met_date = met_day.strftime('%Y-%m-%d')
        if met_date not in met_day_precip:
            met_day_precip[met_date] = []
        met_day_precip[met_date].append(precip)

        # Show hours with significant precip
        if precip >= 30 and '2026-01-2' in cal_date:
            local_str = dt.strftime('%Y-%m-%d %H:%M PST')
            print(f"  {time_str} ({local_str}): {precip}% - Met Day: {met_date}")

    print()
    print("COMPARISON: Calendar Day vs Meteorological Day (MAX aggregation):")
    print("-" * 60)
    print(f"{'Date':<15} {'Calendar Day':<20} {'Met Day (current)':<20}")
    print("-" * 60)

    for date in sorted(set(calendar_day_precip.keys()) | set(met_day_precip.keys())):
        if '2026-01-2' not in date and '2026-02-0' not in date:
            continue
        cal_max = max(calendar_day_precip.get(date, [0]))
        met_max = max(met_day_precip.get(date, [0]))

        diff = ""
        if abs(cal_max - met_max) > 20:
            diff = " <-- MAJOR DIFF"

        print(f"{date:<15} {cal_max:>3}%                {met_max:>3}%{diff}")

    print()
    print("ALTERNATIVE: Using AVERAGE instead of MAX:")
    print("-" * 60)
    for date in sorted(calendar_day_precip.keys()):
        if '2026-01-2' not in date:
            continue
        cal_vals = calendar_day_precip.get(date, [0])
        cal_avg = sum(cal_vals) / len(cal_vals) if cal_vals else 0
        cal_max = max(cal_vals)

        print(f"{date}: AVG={cal_avg:.1f}%, MAX={cal_max}%")

    print()
    print("=" * 70)
    print("RECOMMENDATION:")
    print("=" * 70)
    print("""
The 75% Tuesday precip comes from rain forecast for early morning
Wednesday (Jan 28, 00:00-02:00 PST). Using meteorological day (6am-6am)
attributes this to Tuesday.

OPTIONS:
1. Use CALENDAR day for precip (matches user expectation)
2. Use AVERAGE instead of MAX (less dramatic but may underreport)
3. Use multi-source ensemble (AccuWeather + Open-Meteo + Google avg)

SUGGESTED FIX: Option 1 - Calendar day aggregation for precip
- Keep meteorological day for temps (industry standard)
- Use calendar day for precip (matches user expectation)
""")


def compare_all_sources():
    """Compare precip from all available sources."""
    print()
    print("=" * 70)
    print("ALL SOURCES COMPARISON - Tuesday 2026-01-27")
    print("=" * 70)
    print()

    sources = {
        'AccuWeather': ('accuweather_lkg.json', lambda d: d.get('data', [])),
        'Open-Meteo': ('open_meteo_lkg.json', lambda d: d.get('data', {}).get('daily_forecast', [])),
        'Weather.com': ('weather_com_lkg.json', lambda d: d.get('data', [])),
        'WUnderground': ('wunderground_lkg.json', lambda d: d.get('data', [])),
        'Google': ('google_weather_lkg.json', lambda d: d.get('data', d).get('daily', [])),
    }

    print(f"{'Source':<20} {'Jan 27 Precip':<15} {'Jan 28 Precip':<15}")
    print("-" * 50)

    for name, (filename, extractor) in sources.items():
        path = CACHE_DIR / filename
        if not path.exists():
            print(f"{name:<20} [CACHE NOT FOUND]")
            continue

        with open(path) as f:
            data = json.load(f)

        daily = extractor(data)
        jan27_precip = "N/A"
        jan28_precip = "N/A"

        for day in daily:
            date = day.get('date', '')
            if '2026-01-27' in date:
                jan27_precip = f"{day.get('precip_prob', 'N/A')}%"
            if '2026-01-28' in date:
                jan28_precip = f"{day.get('precip_prob', 'N/A')}%"

        print(f"{name:<20} {jan27_precip:<15} {jan28_precip:<15}")

    print()
    print("CONSENSUS (excluding Google):")
    # Calculate consensus without Google
    values = []
    # AccuWeather: 3%, Open-Meteo: 13%, Weather.com: 0%, WUnderground: 0%
    # These are hardcoded for now based on cache analysis
    values = [3, 13, 0, 0]
    avg = sum(values) / len(values)
    print(f"  Average: {avg:.1f}%")
    print(f"  Google reports: 75%")
    print(f"  Discrepancy: {75 - avg:.1f} percentage points")


if __name__ == "__main__":
    analyze_tuesday_precip()
    compare_all_sources()
