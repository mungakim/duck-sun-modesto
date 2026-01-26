#!/usr/bin/env python3
"""
Debug script to analyze Weather.com HTML structure and find the correct
regex patterns for extracting forecast data including precipitation.
"""

import re
import json
from curl_cffi.requests import Session
from bs4 import BeautifulSoup

SCRAPE_URL = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

def debug_weathercom():
    print("=" * 70)
    print("WEATHER.COM JSON STRUCTURE ANALYSIS")
    print("=" * 70)
    print()

    with Session(impersonate="chrome110") as session:
        # Get cookies first
        session.get("https://weather.com/", timeout=15)
        # Fetch forecast page
        response = session.get(SCRAPE_URL, timeout=30)

    if response.status_code != 200:
        print(f"ERROR: HTTP {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    scripts = soup.find_all('script')

    print(f"Found {len(scripts)} script tags")
    print()

    # Find script with forecast data
    for i, script in enumerate(scripts):
        if not script.string:
            continue

        content = script.string

        # Check for forecast-related content
        has_temp_max = 'temperatureMax' in content
        has_precip = 'precipChance' in content
        has_day_of_week = 'dayOfWeek' in content

        if has_temp_max or has_precip:
            print(f"=== SCRIPT #{i} ===")
            print(f"  Has temperatureMax: {has_temp_max}")
            print(f"  Has precipChance: {has_precip}")
            print(f"  Has dayOfWeek: {has_day_of_week}")
            print(f"  Length: {len(content)} chars")
            print()

            # Try to find the actual patterns
            # Look for different possible formats

            # Pattern 1: Standard JSON array format
            patterns_to_try = [
                (r'"temperatureMax":\s*\[([^\]]+)\]', "temperatureMax standard"),
                (r'"temperatureMax"\s*:\s*\[([^\]]+)\]', "temperatureMax with spaces"),
                (r'temperatureMax["\s:]+\[([^\]]+)\]', "temperatureMax loose"),
                (r'"precipChance":\s*\[([^\]]+)\]', "precipChance standard"),
                (r'"precipChance"\s*:\s*\[([^\]]+)\]', "precipChance with spaces"),
                (r'precipChance["\s:]+\[([^\]]+)\]', "precipChance loose"),
                (r'"dayOfWeek":\s*\[([^\]]+)\]', "dayOfWeek standard"),
                (r'"dayOfWeek"\s*:\s*\[([^\]]+)\]', "dayOfWeek with spaces"),
            ]

            for pattern, name in patterns_to_try:
                match = re.search(pattern, content)
                if match:
                    arr_str = match.group(1)
                    print(f"  [MATCH] {name}:")
                    print(f"    First 200 chars: {arr_str[:200]}...")
                    print()

            # Also look for the overall JSON structure
            # Try to find where the forecast data lives
            json_patterns = [
                r'__NEXT_DATA__[^{]*({.+})</script>',
                r'window\.__data\s*=\s*({.+?});',
                r'"daily":\s*({[^}]+})',
                r'"forecast":\s*({.+?})',
            ]

            print("  Looking for JSON structure patterns...")
            for pattern in json_patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    print(f"    Found pattern: {pattern[:40]}...")
                    # Try to parse as JSON
                    try:
                        data = json.loads(match.group(1))
                        print(f"    Valid JSON with keys: {list(data.keys())[:10]}")
                    except:
                        print(f"    Not valid JSON, first 100 chars: {match.group(1)[:100]}")

            # Print a sample of the content around temperatureMax
            if has_temp_max:
                idx = content.find('temperatureMax')
                if idx > 0:
                    sample = content[max(0, idx-50):idx+200]
                    print(f"\n  Context around 'temperatureMax':")
                    print(f"  ...{sample}...")

            # Print a sample of the content around precipChance
            if has_precip:
                idx = content.find('precipChance')
                if idx > 0:
                    sample = content[max(0, idx-50):idx+200]
                    print(f"\n  Context around 'precipChance':")
                    print(f"  ...{sample}...")

            print()
            print("-" * 70)
            print()


if __name__ == "__main__":
    debug_weathercom()
