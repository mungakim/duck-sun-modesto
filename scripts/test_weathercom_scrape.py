#!/usr/bin/env python3
"""
Test script to explore weather.com HTML structure for precipitation data.
Uses curl_cffi to bypass anti-bot measures.
"""

from curl_cffi.requests import Session
from bs4 import BeautifulSoup
import re

SCRAPE_URL = "https://weather.com/weather/tenday/l/37.6393,-120.9969"

def test_scrape():
    print("=" * 70)
    print("WEATHER.COM HTML STRUCTURE ANALYSIS")
    print("=" * 70)
    print()

    with Session(impersonate="chrome110") as session:
        # Get homepage first for cookies
        print("Getting session cookies...")
        session.get("https://weather.com/", timeout=15)

        # Fetch 10-day forecast
        print(f"Fetching: {SCRAPE_URL}")
        response = session.get(SCRAPE_URL, timeout=30)

    if response.status_code != 200:
        print(f"ERROR: HTTP {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    # Save HTML for inspection
    with open('/tmp/weathercom_sample.html', 'w') as f:
        f.write(soup.prettify())
    print("Saved full HTML to /tmp/weathercom_sample.html")
    print()

    # Find day containers
    print("=== LOOKING FOR FORECAST ELEMENTS ===")
    print()

    # Try various selectors for precipitation
    precip_selectors = [
        ('data-testid="PercentageValue"', lambda s: s.find_all(attrs={"data-testid": "PercentageValue"})),
        ('data-testid="PrecipPercentage"', lambda s: s.find_all(attrs={"data-testid": "PrecipPercentage"})),
        ('data-testid="precipValue"', lambda s: s.find_all(attrs={"data-testid": "precipValue"})),
        ('class contains "precip"', lambda s: s.find_all(class_=lambda x: x and 'precip' in str(x).lower() if x else False)),
        ('class contains "Precip"', lambda s: s.find_all(class_=lambda x: x and 'Precip' in str(x) if x else False)),
        ('span with %', lambda s: [el for el in s.find_all('span') if el.text and '%' in el.text and len(el.text) < 10]),
    ]

    for name, selector in precip_selectors:
        elements = selector(soup)
        if elements:
            print(f"[FOUND] {name}: {len(elements)} elements")
            for i, el in enumerate(elements[:5]):
                text = el.text.strip() if el.text else ""
                classes = el.get('class', [])
                print(f"  [{i}] text='{text}' classes={classes}")
            if len(elements) > 5:
                print(f"  ... and {len(elements) - 5} more")
            print()

    # Look at day card structure
    print("=== DAY CARD STRUCTURE ===")
    day_parts = soup.find_all(attrs={"data-testid": "daypartName"})
    print(f"Found {len(day_parts)} day names")

    # Try to find the parent container of day cards
    if day_parts:
        first_day = day_parts[0]
        # Walk up to find the day card container
        parent = first_day.parent
        for _ in range(5):
            if parent:
                print(f"Parent tag: {parent.name}, classes: {parent.get('class', [])}")
                # Look for precip in this parent
                precip_in_parent = parent.find_all(string=re.compile(r'\d+\s*%'))
                if precip_in_parent:
                    print(f"  Found % values: {[p.strip() for p in precip_in_parent[:3]]}")
                parent = parent.parent

    # Also check for DetailsSummary which often has precip
    print()
    print("=== DETAILS SUMMARY ELEMENTS ===")
    details = soup.find_all(attrs={"data-testid": "wxData"})
    print(f"Found {len(details)} wxData elements")
    for i, d in enumerate(details[:10]):
        print(f"  [{i}] {d.text[:50] if d.text else 'empty'}...")


if __name__ == "__main__":
    test_scrape()
