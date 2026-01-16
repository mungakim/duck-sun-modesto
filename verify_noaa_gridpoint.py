#!/usr/bin/env python3
"""
NOAA Gridpoint Verification Script for KMOD Airport

This script verifies that the hardcoded NOAA gridpoint (STO/45,63) correctly
corresponds to the Modesto City-County Airport - Harry Sham Field (KMOD).

Run this script locally to verify the NOAA location is correct:
    ./venv/Scripts/python.exe verify_noaa_gridpoint.py

Expected output if correct:
    KMOD coordinates (37.62549, -120.9549) -> STO/45,63

If there's a mismatch, the script will show the correct gridpoint to use.
"""

import asyncio
import httpx
import sys
import warnings

# Suppress SSL warnings when using verify=False
warnings.filterwarnings('ignore', message='Unverified HTTPS request')


# KMOD Airport Coordinates (official weather station for Modesto)
# Source: https://forecast.weather.gov/MapClick.php?lat=37.62549&lon=-120.9549
KMOD_LAT = 37.62549
KMOD_LON = -120.9549

# Current hardcoded values in noaa.py
EXPECTED_GRID_ID = "STO"
EXPECTED_GRID_X = 45
EXPECTED_GRID_Y = 63

HEADERS = {
    "User-Agent": "(duck-sun-modesto, github.com/user/duck-sun-modesto)",
    "Accept": "application/geo+json"
}


async def verify_gridpoint():
    """Verify the gridpoint matches KMOD coordinates."""
    print("=" * 70)
    print("NOAA Gridpoint Verification for KMOD Airport")
    print("=" * 70)
    print()
    print(f"KMOD Airport Location:")
    print(f"  Latitude:  {KMOD_LAT}N")
    print(f"  Longitude: {KMOD_LON}W")
    print()
    print(f"Expected Gridpoint (from noaa.py):")
    print(f"  {EXPECTED_GRID_ID}/{EXPECTED_GRID_X},{EXPECTED_GRID_Y}")
    print()

    # Step 1: Look up gridpoint from KMOD coordinates
    points_url = f"https://api.weather.gov/points/{KMOD_LAT},{KMOD_LON}"
    print(f"Querying NOAA Points API...")
    print(f"  URL: {points_url}")
    print()

    try:
        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            resp = await client.get(points_url, headers=HEADERS)

            if resp.status_code != 200:
                print(f"ERROR: Points API returned HTTP {resp.status_code}")
                print(f"Response: {resp.text[:500]}")
                return False

            data = resp.json()
            props = data.get('properties', {})

            actual_grid_id = props.get('gridId')
            actual_grid_x = props.get('gridX')
            actual_grid_y = props.get('gridY')
            forecast_url = props.get('forecast')
            forecast_hourly_url = props.get('forecastHourly')

            print(f"API Response:")
            print(f"  Grid ID: {actual_grid_id}")
            print(f"  Grid X:  {actual_grid_x}")
            print(f"  Grid Y:  {actual_grid_y}")
            print(f"  Forecast URL: {forecast_url}")
            print()

            # Step 2: Compare with expected values
            match = (
                actual_grid_id == EXPECTED_GRID_ID and
                actual_grid_x == EXPECTED_GRID_X and
                actual_grid_y == EXPECTED_GRID_Y
            )

            if match:
                print("=" * 70)
                print("VERIFIED: Gridpoint matches KMOD coordinates!")
                print(f"  KMOD ({KMOD_LAT}, {KMOD_LON}) -> {actual_grid_id}/{actual_grid_x},{actual_grid_y}")
                print("=" * 70)
                return True
            else:
                print("=" * 70)
                print("MISMATCH DETECTED!")
                print(f"  Expected: {EXPECTED_GRID_ID}/{EXPECTED_GRID_X},{EXPECTED_GRID_Y}")
                print(f"  Actual:   {actual_grid_id}/{actual_grid_x},{actual_grid_y}")
                print()
                print("ACTION REQUIRED: Update noaa.py with correct gridpoint:")
                print(f'  EXPECTED_GRID_ID = "{actual_grid_id}"')
                print(f'  EXPECTED_GRID_X = {actual_grid_x}')
                print(f'  EXPECTED_GRID_Y = {actual_grid_y}')
                print("=" * 70)
                return False

    except Exception as e:
        print(f"ERROR: {e}")
        return False


async def fetch_and_compare_forecasts():
    """Fetch and compare forecast periods vs gridpoint model data."""
    print()
    print("=" * 70)
    print("Comparing Forecast Periods vs Gridpoint Model")
    print("=" * 70)
    print()

    gridpoint_url = f"https://api.weather.gov/gridpoints/{EXPECTED_GRID_ID}/{EXPECTED_GRID_X},{EXPECTED_GRID_Y}"
    forecast_url = f"{gridpoint_url}/forecast"

    try:
        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            # Fetch forecast periods (matches weather.gov)
            print(f"Fetching forecast periods...")
            print(f"  URL: {forecast_url}")
            resp = await client.get(forecast_url, headers=HEADERS)

            if resp.status_code != 200:
                print(f"ERROR: Forecast API returned HTTP {resp.status_code}")
                return

            forecast_data = resp.json()
            periods = forecast_data.get('properties', {}).get('periods', [])

            # Extract daily high/low from periods
            print()
            print("Forecast Periods (matches weather.gov website):")
            print("-" * 50)
            for p in periods[:8]:
                name = p.get('name', '')
                temp = p.get('temperature')
                unit = p.get('temperatureUnit', 'F')
                is_day = p.get('isDaytime')
                temp_type = "High" if is_day else "Low"
                print(f"  {name:15} {temp_type}: {temp}{unit}")

            # Fetch gridpoint model data
            print()
            print(f"Fetching gridpoint model data...")
            print(f"  URL: {gridpoint_url}")
            resp = await client.get(gridpoint_url, headers=HEADERS)

            if resp.status_code != 200:
                print(f"ERROR: Gridpoint API returned HTTP {resp.status_code}")
                return

            gridpoint_data = resp.json()
            temps = gridpoint_data.get('properties', {}).get('temperature', {}).get('values', [])

            print()
            print("Gridpoint Model (raw numerical data):")
            print("-" * 50)
            print(f"  Retrieved {len(temps)} hourly temperature records")
            if temps:
                # Show first few
                for t in temps[:5]:
                    time = t.get('validTime', '').split('/')[0]
                    temp_c = t.get('value')
                    temp_f = round(temp_c * 1.8 + 32) if temp_c else None
                    print(f"  {time}: {temp_c}C ({temp_f}F)")
                print("  ...")

    except Exception as e:
        print(f"ERROR: {e}")


async def main():
    """Main entry point."""
    verified = await verify_gridpoint()

    if verified:
        await fetch_and_compare_forecasts()

    print()
    print("Done.")
    return 0 if verified else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
