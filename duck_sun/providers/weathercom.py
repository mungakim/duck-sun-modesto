"""
Weather.com Provider for Duck Sun Modesto

ARCHITECTURE: Cache-based with text parsing for easy updates.

Weather.com is JavaScript-rendered and blocks automated scraping.
This provider uses a TEXT PARSER approach:
1. User pastes forecast text from weather.com
2. Parser extracts dates, temps, conditions, precip
3. Cache stores parsed data for 24 hours

To update: python -m duck_sun.providers.weathercom --update
Then paste the 10-day forecast text from weather.com
"""

import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, TypedDict

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "weathercom_cache.json"
CACHE_TTL_HOURS = 24  # Extended TTL for manual ground truth


class WeatherComDay(TypedDict):
    """Daily forecast data from Weather.com."""
    date: str
    high_f: Optional[int]
    low_f: int
    high_c: Optional[float]
    low_c: float
    condition: str
    precip_prob: int


# Day name to offset mapping (relative to today)
DAY_NAMES = {
    'sun': 0, 'sunday': 0,
    'mon': 1, 'monday': 1,
    'tue': 2, 'tuesday': 2,
    'wed': 3, 'wednesday': 3,
    'thu': 4, 'thursday': 4,
    'fri': 5, 'friday': 5,
    'sat': 6, 'saturday': 6,
}


def parse_weathercom_text(text: str) -> List[WeatherComDay]:
    """
    Parse Weather.com 10-day forecast text into structured data.

    Expected format (copy-pasted from weather.com):
    ```
    Tonight
    Cloudy
    --
    /44°
    5%
    Wed 17
    AM Clouds/PM Sun
    64°
    /47°
    22%
    ...
    ```

    Returns:
        List of WeatherComDay dicts
    """
    results: List[WeatherComDay] = []
    lines = [line.strip() for line in text.strip().split('\n') if line.strip()]

    today = datetime.now()
    today_weekday = today.weekday()  # Monday = 0, Sunday = 6

    i = 0
    while i < len(lines):
        line = lines[i].lower()

        # Check if this line is a day header (Tonight, Today, or Day Name + Date)
        is_tonight = 'tonight' in line
        is_today = line == 'today'
        day_match = re.match(r'^(sun|mon|tue|wed|thu|fri|sat)\w*\s+(\d{1,2})$', line, re.IGNORECASE)

        if is_tonight or is_today or day_match:
            # Found a day header - extract forecast data
            try:
                if is_tonight or is_today:
                    # Tonight/Today is current date
                    forecast_date = today
                    day_num = None
                elif day_match:
                    day_name = day_match.group(1).lower()
                    day_num = int(day_match.group(2))

                    # Calculate date from day name and day number
                    # Find the next occurrence of this weekday
                    target_weekday = DAY_NAMES.get(day_name[:3], 0)

                    # Convert to Python weekday (Mon=0) from weather.com (Sun=0)
                    python_weekday = (target_weekday + 6) % 7 if target_weekday == 0 else target_weekday - 1

                    days_ahead = (python_weekday - today_weekday) % 7
                    if days_ahead == 0 and day_num != today.day:
                        days_ahead = 7  # Next week

                    forecast_date = today + timedelta(days=days_ahead)

                    # Adjust if the day number doesn't match (cross-month)
                    if forecast_date.day != day_num:
                        # Try to find correct date within next 10 days
                        for offset in range(10):
                            check_date = today + timedelta(days=offset)
                            if check_date.day == day_num and check_date.strftime('%a').lower()[:3] == day_name[:3]:
                                forecast_date = check_date
                                break

                # Next line should be condition
                i += 1
                if i >= len(lines):
                    break
                condition = lines[i]

                # Next line should be high temp (or -- for tonight)
                i += 1
                if i >= len(lines):
                    break
                high_line = lines[i]
                high_f = None
                if high_line != '--':
                    high_match = re.search(r'(\d+)', high_line)
                    if high_match:
                        high_f = int(high_match.group(1))

                # Next line should be low temp (with / prefix)
                i += 1
                if i >= len(lines):
                    break
                low_line = lines[i]
                low_match = re.search(r'(\d+)', low_line)
                if not low_match:
                    i += 1
                    continue
                low_f = int(low_match.group(1))

                # Next line should be precip probability
                i += 1
                if i >= len(lines):
                    break
                precip_line = lines[i]
                precip_match = re.search(r'(\d+)', precip_line)
                precip_prob = int(precip_match.group(1)) if precip_match else 0

                # Calculate Celsius
                high_c = round((high_f - 32) * 5 / 9, 1) if high_f else None
                low_c = round((low_f - 32) * 5 / 9, 1)

                results.append({
                    'date': forecast_date.strftime('%Y-%m-%d'),
                    'high_f': high_f,
                    'low_f': low_f,
                    'high_c': high_c,
                    'low_c': low_c,
                    'condition': condition,
                    'precip_prob': precip_prob
                })

            except (ValueError, IndexError) as e:
                logger.warning(f"[parse_weathercom_text] Parse error at line {i}: {e}")

        i += 1

    # Remove duplicates (keep first occurrence of each date)
    seen_dates = set()
    unique_results = []
    for day in results:
        if day['date'] not in seen_dates:
            seen_dates.add(day['date'])
            unique_results.append(day)

    # Sort by date
    unique_results.sort(key=lambda x: x['date'])

    return unique_results


class WeatherComProvider:
    """
    Weather.com provider for Modesto, CA forecasts.

    Due to Weather.com being JavaScript-rendered, this provider:
    1. Uses a TEXT PARSER to convert pasted forecast data
    2. Caches parsed data for 24 hours
    3. Provides CLI update tool: python -m duck_sun.providers.weathercom --update

    Weight in ensemble: 2.0 (user baseline reference)
    """

    # Weather.com URL for reference
    WEATHER_COM_URL = "https://weather.com/weather/tenday/l/37.6391,-120.9969"

    def __init__(self):
        logger.info("[WeatherComProvider] Initializing provider...")
        CACHE_DIR.mkdir(exist_ok=True)

    def _load_cache(self) -> Optional[dict]:
        """Load cached data if within TTL."""
        if not CACHE_FILE.exists():
            logger.warning("[WeatherComProvider] No cache file found")
            return None

        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
            age = datetime.now() - cached_time
            age_minutes = age.total_seconds() / 60

            logger.info(f"[WeatherComProvider] Cache age: {age_minutes:.1f} minutes")

            if age <= timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"[WeatherComProvider] Cache VALID (TTL: {CACHE_TTL_HOURS}h)")
                return cache
            else:
                logger.warning(f"[WeatherComProvider] Cache EXPIRED (age: {age_minutes/60:.1f}h > {CACHE_TTL_HOURS}h)")
                # Still return expired cache - better than nothing
                return cache

        except Exception as e:
            logger.warning(f"[WeatherComProvider] Cache load error: {e}")
            return None

    def _save_cache(self, data: List[WeatherComDay]) -> bool:
        """Save forecast data to cache."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'source': 'weather.com (text parser)',
                'ttl_hours': CACHE_TTL_HOURS,
                'data': data
            }

            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)

            logger.info(f"[WeatherComProvider] Cache saved: {len(data)} days")
            return True

        except Exception as e:
            logger.error(f"[WeatherComProvider] Cache save failed: {e}")
            return False

    async def fetch_forecast(self, force_refresh: bool = False) -> Optional[List[WeatherComDay]]:
        """
        Fetch Weather.com forecast data from cache.

        Args:
            force_refresh: Ignored (cache-only provider)

        Returns:
            List of WeatherComDay dicts, or None if no cache
        """
        cache = self._load_cache()
        if cache and cache.get('data'):
            logger.info("[WeatherComProvider] CACHE HIT - Returning cached data")
            return cache['data']

        logger.warning("[WeatherComProvider] NO CACHE - Run: python -m duck_sun.providers.weathercom --update")
        return None

    def update_from_text(self, text: str) -> bool:
        """
        Update cache from pasted Weather.com forecast text.

        Args:
            text: Raw text copied from weather.com 10-day page

        Returns:
            True if update successful
        """
        try:
            results = parse_weathercom_text(text)

            if not results:
                logger.error("[WeatherComProvider] No forecast data parsed from text")
                return False

            logger.info(f"[WeatherComProvider] Parsed {len(results)} days from text")
            return self._save_cache(results)

        except Exception as e:
            logger.error(f"[WeatherComProvider] Update failed: {e}")
            return False

    def get_status(self) -> dict:
        """Get provider status information."""
        cache = self._load_cache()
        cache_age = None
        if cache:
            try:
                cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
                cache_age = (datetime.now() - cached_time).total_seconds() / 3600
            except:
                pass

        return {
            "provider": "Weather.com",
            "status": "cache_based",
            "cache_valid": cache is not None,
            "cache_age_hours": round(cache_age, 1) if cache_age else None,
            "days_cached": len(cache.get('data', [])) if cache else 0,
            "url": self.WEATHER_COM_URL
        }


def interactive_update():
    """Interactive CLI for updating Weather.com cache."""
    print("=" * 60)
    print("  WEATHER.COM CACHE UPDATER")
    print("=" * 60)
    print()
    print("Instructions:")
    print("1. Go to: https://weather.com/weather/tenday/l/37.6391,-120.9969")
    print("2. Select and copy the 10-day forecast text")
    print("3. Paste below (press Enter twice when done):")
    print()
    print("-" * 60)

    lines = []
    empty_count = 0

    try:
        while empty_count < 2:
            line = input()
            if line.strip() == '':
                empty_count += 1
            else:
                empty_count = 0
                lines.append(line)
    except EOFError:
        pass

    if not lines:
        print("\nNo input received. Aborting.")
        return False

    text = '\n'.join(lines)
    print("-" * 60)
    print()

    # Parse and save
    provider = WeatherComProvider()

    print("Parsing forecast data...")
    results = parse_weathercom_text(text)

    if not results:
        print("ERROR: Could not parse any forecast data from input.")
        return False

    print(f"\nParsed {len(results)} days:")
    print("-" * 50)
    for day in results:
        hi = day['high_f'] if day['high_f'] else "--"
        print(f"  {day['date']}: Hi={hi}F, Lo={day['low_f']}F, "
              f"Precip={day['precip_prob']}%, {day['condition']}")

    print()
    confirm = input("Save to cache? [Y/n]: ").strip().lower()

    if confirm in ('', 'y', 'yes'):
        if provider._save_cache(results):
            print("\n✓ Cache updated successfully!")
            return True
        else:
            print("\n✗ Failed to save cache.")
            return False
    else:
        print("\nCancelled.")
        return False


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    if '--update' in sys.argv:
        # Interactive update mode
        success = interactive_update()
        sys.exit(0 if success else 1)

    elif '--parse' in sys.argv:
        # Parse from stdin
        print("Paste Weather.com forecast text (Ctrl+D when done):")
        text = sys.stdin.read()
        results = parse_weathercom_text(text)
        print(json.dumps(results, indent=2))

    else:
        # Status/test mode
        async def test():
            print("=" * 60)
            print("  WEATHER.COM PROVIDER STATUS")
            print("=" * 60)

            provider = WeatherComProvider()

            print("\n[STATUS]")
            status = provider.get_status()
            for key, value in status.items():
                print(f"  {key}: {value}")

            print("\n[CACHE DATA]")
            data = await provider.fetch_forecast()

            if data:
                print(f"  Found {len(data)} days in cache:")
                print("-" * 50)
                for day in data:
                    hi = day['high_f'] if day['high_f'] else "--"
                    print(f"  {day['date']}: Hi={hi}F, Lo={day['low_f']}F, "
                          f"Precip={day['precip_prob']}%, {day['condition']}")
            else:
                print("  No cache data available.")
                print("\n  To update, run:")
                print("    python -m duck_sun.providers.weathercom --update")

            print("\n" + "=" * 60)

        asyncio.run(test())
