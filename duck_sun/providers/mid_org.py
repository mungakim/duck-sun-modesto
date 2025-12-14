"""
MID.org (Modesto Irrigation District) Weather Provider

Fetches local weather data from the MID.org public REST API.
API discovered at: https://midapi.websupport.expert/

Available Endpoints:
- /weather/twoday/summary - Today/Yesterday High/Low/Rain
- /weather/twoday/detail - Hourly breakdown for 48 hours
- /weather/widget - Today's data + historical records

The MID data is valuable because:
- Local microclimate data specific to Modesto
- Historical records since 1888 (rainfall) and 1939 (temperatures)
- Ground truth from downtown Modesto station
"""

import httpx
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("outputs")
CACHE_FILE = CACHE_DIR / "mid_org_cache.json"
CACHE_TTL_HOURS = 1

# MID.org API Base URL
MID_API_BASE = "https://midapi.websupport.expert"


class MIDOrgProvider:
    """
    Modesto Irrigation District local weather data provider.

    Uses the public REST API to fetch:
    - 48-hour weather summary (today/yesterday high/low/rain)
    - Historical records for the current date
    - Hourly detail if needed

    Weight in ensemble: 2.0 (local ground truth)
    """

    HEADERS = {
        "User-Agent": "duck-sun-modesto/1.0 (solar forecasting for Modesto grid)",
        "Accept": "application/json"
    }

    def __init__(self):
        logger.info("[MIDOrgProvider] Initializing provider (REST API mode)...")
        CACHE_DIR.mkdir(exist_ok=True)

    def _load_cache(self) -> Optional[dict]:
        """Load cached data if within TTL."""
        if not CACHE_FILE.exists():
            return None

        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            cached_time = datetime.fromisoformat(cache.get('timestamp', ''))
            age = datetime.now() - cached_time

            if age <= timedelta(hours=CACHE_TTL_HOURS):
                age_mins = age.total_seconds() / 60
                logger.info(f"[MIDOrgProvider] Cache VALID (age: {age_mins:.1f} min)")
                return cache
            else:
                logger.info("[MIDOrgProvider] Cache EXPIRED")
                return None

        except Exception as e:
            logger.warning(f"[MIDOrgProvider] Cache load error: {e}")
            return None

    def _save_cache(self, data: Dict[str, Any]) -> bool:
        """Save weather data to cache."""
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'source': 'midapi.websupport.expert',
                'data': data
            }

            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)

            logger.info(f"[MIDOrgProvider] Cache saved -> {CACHE_FILE}")
            return True

        except Exception as e:
            logger.error(f"[MIDOrgProvider] Cache save failed: {e}")
            return False

    async def fetch_48hr_summary(self, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch 48-hour weather summary from MID.org REST API.

        Returns dict with structure:
        {
            "today": {"high": "44", "low": "38", "rain": "0.00", "datestring": "..."},
            "yesterday": {"high": "44", "low": "39", "rain": "0.00", "datestring": "..."},
            "record_high_temp": 68,
            "record_high_year": 1969,
            "record_low_temp": 22,
            "record_low_year": 1949,
            ...
        }
        """
        # Check cache first
        if not force_refresh:
            cache = self._load_cache()
            if cache and cache.get('data'):
                logger.info("[MIDOrgProvider] CACHE HIT - Returning cached data")
                return cache['data']

        logger.info("[MIDOrgProvider] Fetching from MID API...")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                # Fetch 48-hour summary
                summary_url = f"{MID_API_BASE}/weather/twoday/summary"
                summary_resp = await client.get(summary_url, headers=self.HEADERS)

                if summary_resp.status_code != 200:
                    logger.warning(f"[MIDOrgProvider] Summary API returned {summary_resp.status_code}")
                    return None

                summary_data = summary_resp.json()
                logger.info(f"[MIDOrgProvider] Got 48hr summary: Today {summary_data.get('today', {}).get('high')}/{summary_data.get('today', {}).get('low')}F")

                # Fetch widget data for historical records
                widget_url = f"{MID_API_BASE}/weather/widget"
                widget_resp = await client.get(widget_url, headers=self.HEADERS)

                if widget_resp.status_code == 200:
                    widget_data = widget_resp.json()
                    # Merge widget data (historical records) into summary
                    summary_data['record_high_temp'] = widget_data.get('record_high_temp')
                    summary_data['record_high_year'] = widget_data.get('record_high_year')
                    summary_data['record_low_temp'] = widget_data.get('record_low_temp')
                    summary_data['record_low_year'] = widget_data.get('record_low_year')
                    summary_data['avg_high_temp'] = widget_data.get('avg_high_temp')
                    summary_data['avg_low_temp'] = widget_data.get('avg_low_temp')
                    logger.info(f"[MIDOrgProvider] Got widget data: Records Hi {widget_data.get('record_high_temp')}F ({widget_data.get('record_high_year')})")

                # Cache the combined data
                self._save_cache(summary_data)
                return summary_data

        except httpx.TimeoutException:
            logger.warning("[MIDOrgProvider] Request timed out")
            return None
        except Exception as e:
            logger.warning(f"[MIDOrgProvider] Fetch failed: {e}")
            return None

    async def fetch_48hr_detail(self) -> Optional[list]:
        """
        Fetch detailed hourly data for the past 48 hours.

        Returns list of hourly records with temperature, wind, barometer, rain.
        """
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                detail_url = f"{MID_API_BASE}/weather/twoday/detail"
                resp = await client.get(detail_url, headers=self.HEADERS)

                if resp.status_code != 200:
                    logger.warning(f"[MIDOrgProvider] Detail API returned {resp.status_code}")
                    return None

                data = resp.json()
                logger.info(f"[MIDOrgProvider] Got {len(data)} hourly detail records")
                return data

        except Exception as e:
            logger.warning(f"[MIDOrgProvider] Detail fetch failed: {e}")
            return None

    def get_status(self) -> dict:
        """Get provider status information."""
        cache = self._load_cache()
        return {
            "provider": "MID.org",
            "status": "active",
            "api_base": MID_API_BASE,
            "cache_available": cache is not None,
            "endpoints": ["/weather/twoday/summary", "/weather/widget", "/weather/twoday/detail"]
        }


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.DEBUG)

    async def test():
        print("=" * 60)
        print("  MID.ORG PROVIDER TEST (REST API)")
        print("=" * 60)

        provider = MIDOrgProvider()

        print("\n[PROVIDER STATUS]")
        status = provider.get_status()
        for key, value in status.items():
            print(f"  {key}: {value}")

        print("\n[FETCHING 48-HOUR SUMMARY]")
        data = await provider.fetch_48hr_summary(force_refresh=True)

        if data:
            print(f"\n[RESULTS] MID.org Weather Data:")
            print("-" * 50)

            today = data.get('today', {})
            yest = data.get('yesterday', {})

            print(f"  TODAY:     High: {today.get('high')}F  Low: {today.get('low')}F  Rain: {today.get('rain')}\"")
            print(f"  YESTERDAY: High: {yest.get('high')}F  Low: {yest.get('low')}F  Rain: {yest.get('rain')}\"")

            if 'record_high_temp' in data:
                print(f"\n  RECORDS:")
                print(f"    Record High: {data.get('record_high_temp')}F ({data.get('record_high_year')})")
                print(f"    Record Low:  {data.get('record_low_temp')}F ({data.get('record_low_year')})")
                print(f"    Avg High:    {data.get('avg_high_temp')}F")
                print(f"    Avg Low:     {data.get('avg_low_temp')}F")
        else:
            print("\n[RESULT] No data available")

        print("\n[FETCHING 48-HOUR DETAIL]")
        detail = await provider.fetch_48hr_detail()
        if detail:
            print(f"  Got {len(detail)} hourly records")
            if detail:
                print(f"  Sample: {detail[0]}")

        print("\n" + "=" * 60)

    asyncio.run(test())
