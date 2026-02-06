"""
METAR Provider for Duck Sun Modesto

Fetches real-time ground truth weather observations from KMOD
(Modesto City-County Airport). METAR data is the actual measured
weather at the airport, providing validation against forecast models.

This is our "look outside" reality check - actual sensor data
to verify if the models are matching real conditions.
"""

import httpx
import logging
import os
import re
from datetime import datetime
from typing import Optional, TypedDict

# SSL: Use OS certificate store for PyInstaller exe compatibility
try:
    from duck_sun.ssl_helper import get_ca_bundle_for_curl as get_ca_bundle
except ImportError:
    def get_ca_bundle():
        return os.getenv("DUCK_SUN_CA_BUNDLE", True)

logger = logging.getLogger(__name__)


class MetarObservation(TypedDict):
    raw: str  # Raw METAR text
    station: str
    observation_time: str
    temp_c: Optional[float]
    dewpoint_c: Optional[float]
    wind_speed_kt: Optional[int]
    wind_dir: Optional[int]
    visibility_sm: Optional[float]
    sky_condition: str


class MetarProvider:
    """
    Provider for real-time METAR observations from KMOD.

    KMOD = Modesto City-County Airport
    Data source: NWS Text Data Server (tgftp.nws.noaa.gov)
    """

    # KMOD = Modesto City-County Airport
    METAR_URL = "https://tgftp.nws.noaa.gov/data/observations/metar/stations/KMOD.TXT"

    def __init__(self):
        self.last_observation: Optional[MetarObservation] = None

    def fetch(self) -> Optional[str]:
        """
        Fetch raw METAR text from KMOD.

        Returns:
            Raw METAR string, or None if fetch fails.
        """
        logger.info("[MetarProvider] Fetching KMOD observation...")

        try:
            with httpx.Client(timeout=10.0, verify=get_ca_bundle()) as client:
                resp = client.get(self.METAR_URL)

                if resp.status_code != 200:
                    logger.warning(f"[MetarProvider] HTTP {resp.status_code}")
                    return None

                raw_text = resp.text.strip()
                logger.info(f"[MetarProvider] Raw METAR: {raw_text[:100]}...")

                return raw_text

        except httpx.TimeoutException:
            logger.warning("[MetarProvider] Request timed out")
            return None
        except httpx.RequestError as e:
            logger.warning(f"[MetarProvider] Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"[MetarProvider] Unexpected error: {e}", exc_info=True)
            return None

    def fetch_parsed(self) -> Optional[MetarObservation]:
        """
        Fetch and parse METAR data into structured format.

        Returns:
            Parsed MetarObservation, or None if fetch/parse fails.
        """
        raw = self.fetch()

        if not raw:
            return None

        return self.parse_metar(raw)

    def parse_metar(self, raw_text: str) -> Optional[MetarObservation]:
        """
        Parse raw METAR text into structured data.

        METAR format example:
        2025/01/15 15:53
        KMOD 151553Z 00000KT 10SM CLR 12/06 A3025

        Args:
            raw_text: Raw METAR string from NWS

        Returns:
            Parsed MetarObservation
        """
        try:
            lines = raw_text.strip().split('\n')

            # First line is often the timestamp
            obs_time = ""
            metar_line = ""

            for line in lines:
                line = line.strip()
                if line.startswith('KMOD'):
                    metar_line = line
                elif '/' in line and len(line) <= 20:
                    obs_time = line

            if not metar_line:
                logger.warning("[MetarProvider] No KMOD line found in METAR")
                return None

            # Parse temperature (format: TT/DD where TT=temp, DD=dewpoint)
            # Negative temps use M prefix (e.g., M02 = -2)
            temp_c = None
            dewpoint_c = None
            temp_match = re.search(r'\s(M?\d{2})/(M?\d{2})\s', metar_line)
            if temp_match:
                t_str, d_str = temp_match.groups()
                temp_c = -int(t_str[1:]) if t_str.startswith('M') else int(t_str)
                dewpoint_c = -int(d_str[1:]) if d_str.startswith('M') else int(d_str)

            # Parse wind (format: dddssKT or dddssGggKT)
            wind_speed = None
            wind_dir = None
            wind_match = re.search(r'(\d{3}|VRB)(\d{2,3})(?:G\d{2,3})?KT', metar_line)
            if wind_match:
                dir_str, speed_str = wind_match.groups()
                wind_speed = int(speed_str)
                wind_dir = int(dir_str) if dir_str != 'VRB' else None

            # Parse visibility (format: NNsm or NNNSM)
            visibility = None
            vis_match = re.search(r'(\d+)SM', metar_line)
            if vis_match:
                visibility = float(vis_match.group(1))

            # Parse sky condition
            sky = "Unknown"
            if 'CLR' in metar_line or 'SKC' in metar_line:
                sky = "Clear"
            elif 'FEW' in metar_line:
                sky = "Few Clouds"
            elif 'SCT' in metar_line:
                sky = "Scattered"
            elif 'BKN' in metar_line:
                sky = "Broken"
            elif 'OVC' in metar_line:
                sky = "Overcast"
            elif 'VV' in metar_line:
                sky = "Vertical Visibility (Fog/Low Clouds)"

            # Check for special weather
            if 'FG' in metar_line:
                sky = "FOG - " + sky
            if 'BR' in metar_line:
                sky = "MIST - " + sky
            if 'HZ' in metar_line:
                sky = "HAZE - " + sky

            observation: MetarObservation = {
                "raw": metar_line,
                "station": "KMOD",
                "observation_time": obs_time,
                "temp_c": temp_c,
                "dewpoint_c": dewpoint_c,
                "wind_speed_kt": wind_speed,
                "wind_dir": wind_dir,
                "visibility_sm": visibility,
                "sky_condition": sky
            }

            self.last_observation = observation
            logger.info(f"[MetarProvider] Parsed: {temp_c}C, {sky}, wind {wind_speed}kt")

            return observation

        except Exception as e:
            logger.error(f"[MetarProvider] Parse error: {e}", exc_info=True)
            return None

    async def fetch_async(self) -> Optional[str]:
        """
        Async version of fetch for concurrent data gathering.
        """
        logger.info("[MetarProvider] Async fetch KMOD observation...")

        try:
            async with httpx.AsyncClient(timeout=10.0, verify=get_ca_bundle()) as client:
                resp = await client.get(self.METAR_URL)

                if resp.status_code != 200:
                    logger.warning(f"[MetarProvider] HTTP {resp.status_code}")
                    return None

                return resp.text.strip()

        except Exception as e:
            logger.warning(f"[MetarProvider] Async fetch failed: {e}")
            return None


if __name__ == "__main__":
    # Test the provider
    logging.basicConfig(level=logging.INFO)

    provider = MetarProvider()

    print("\n=== Raw METAR ===")
    raw = provider.fetch()
    if raw:
        print(raw)

    print("\n=== Parsed METAR ===")
    parsed = provider.fetch_parsed()
    if parsed:
        print(f"Station: {parsed['station']}")
        print(f"Time: {parsed['observation_time']}")
        print(f"Temperature: {parsed['temp_c']}C")
        print(f"Dewpoint: {parsed['dewpoint_c']}C")
        print(f"Wind: {parsed['wind_dir']}deg @ {parsed['wind_speed_kt']}kt")
        print(f"Visibility: {parsed['visibility_sm']} SM")
        print(f"Sky: {parsed['sky_condition']}")
    else:
        print("Failed to parse METAR data")
