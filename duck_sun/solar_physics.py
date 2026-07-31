"""
Solar Physics Module for Duck Sun Modesto

Hybridizes:
1. Open-Meteo Physics (Direct Normal Irradiance + Diffuse)
2. Google MetNet-3 (Precise Cloud Timing)

Strategy:
- Base: Use Open-Meteo's GHI (Global Horizontal Irradiance)
- Modulation: If Google predicts >70% clouds, apply a damping factor to the Base.
- Result: Physics-based intensity with AI-based timing.

This module replaces the old cloud-percentage guessing with real physics + AI timing.
"""

import math
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Modesto, CA coordinates
MODESTO_LAT = 37.6391
MODESTO_LON = -120.9969

# Maximum expected GHI for the region
MAX_GHI = 900  # W/m2


def calculate_theoretical_max_ghi(hour: int, day_of_year: int, lat: float = MODESTO_LAT) -> float:
    """
    Calculate theoretical maximum clear-sky GHI for a given hour and day.

    Uses simplified solar position model for Modesto, CA.

    Args:
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)
        lat: Latitude in degrees

    Returns:
        Theoretical max GHI in W/m2 (0 if sun is below horizon)
    """
    # Solar declination angle (Earth's axial tilt effect)
    declination = 23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365))

    # Hour angle (solar noon ~12:30 PST for Modesto)
    hour_angle = 15 * (hour - 12.5)

    # Solar elevation calculation
    lat_rad = math.radians(lat)
    decl_rad = math.radians(declination)
    hour_rad = math.radians(hour_angle)

    sin_elevation = (math.sin(lat_rad) * math.sin(decl_rad) +
                     math.cos(lat_rad) * math.cos(decl_rad) * math.cos(hour_rad))

    if sin_elevation <= 0:
        return 0.0  # Sun below horizon

    # Clear-sky GHI model
    # Max GHI at solar noon: ~600 W/m2 in winter, ~1000 W/m2 in summer
    seasonal_factor = 0.7 + 0.3 * math.cos(math.radians((day_of_year - 172) * 360 / 365))
    max_ghi = MAX_GHI * seasonal_factor

    # GHI based on elevation angle
    ghi = max_ghi * sin_elevation

    return min(ghi, MAX_GHI)


def calculate_solar_from_cloud_cover(
    cloud_cover: float,
    hour: int,
    day_of_year: int,
    lat: float = MODESTO_LAT
) -> float:
    """
    Derive irradiance from cloud cover alone against the clear-sky ceiling.

    This is the single irradiance model used across the report - both the
    Excel solar grid and the physics engine call it, so the grid and the JSON
    outlook can never disagree.

    Attenuation is linear in cloud fraction down to 30% of clear sky at fully
    overcast, which is the usual diffuse floor for a Central Valley overcast.

    Args:
        cloud_cover: Cloud cover percentage 0-100 (from Google MetNet-3)
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)
        lat: Latitude in degrees

    Returns:
        Irradiance in W/m2 (0 if the sun is below the horizon)
    """
    max_theoretical = calculate_theoretical_max_ghi(hour, day_of_year, lat)
    if max_theoretical <= 0:
        return 0.0

    cloud_fraction = max(0.0, min(100.0, float(cloud_cover))) / 100.0
    attenuation = 1.0 - (0.7 * cloud_fraction)
    return max_theoretical * attenuation


def calculate_hybrid_solar(
    om_radiation: float,
    google_cloud: Optional[int],
    hour: int,
    day_of_year: int
) -> float:
    """
    Calculate solar irradiance, GOOGLE-FIRST.

    Priority (Jul 2026 - Google is the preferred solar source everywhere):
    1. Google MetNet-3 cloud cover against the clear-sky ceiling. Google's
       satellite/radar fusion resolves cloud TIMING far better than a physics
       model's radiative transfer, and timing is what drives the duck curve.
    2. Open-Meteo shortwave radiation - ONLY when Google has no cloud value for
       this hour (google_cloud is None). Pass None, not a placeholder: a
       hardcoded default like 50 is indistinguishable from a real 50% reading
       and would silently fabricate a half-clouded sky.
    3. Zero, if neither source has anything.

    The function keeps its historical name because callers and tests reference
    it, but it is no longer a blend - Open-Meteo is a fallback, not a baseline.

    Args:
        om_radiation: Watts/m2 from Open-Meteo (fallback only)
        google_cloud: Cloud cover percentage from Google (0-100), or None if absent
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)

    Returns:
        Solar irradiance in W/m2
    """
    max_theoretical = calculate_theoretical_max_ghi(hour, day_of_year)
    if max_theoretical <= 0:
        # Sun is down - no solar production
        return 0.0

    # 1. GOOGLE PRIMARY
    if google_cloud is not None:
        return calculate_solar_from_cloud_cover(google_cloud, hour, day_of_year)

    # 2. OPEN-METEO FALLBACK (only when Google has nothing for this hour)
    if om_radiation and om_radiation > 0:
        logger.debug(
            f"[solar_physics] No Google cloud for hour {hour} - "
            f"falling back to Open-Meteo radiation ({om_radiation:.0f}W)"
        )
        return float(om_radiation)

    return 0.0


def calculate_tule_fog_penalty(
    temp_c: float,
    dewpoint_c: float,
    wind_speed_kmh: float,
    hour: int
) -> float:
    """
    Calculate Tule Fog specific solar penalty.

    Tule Fog conditions (Central Valley radiation fog):
    - Temperature near or below dewpoint (spread < 1.5C)
    - Light winds (<5 km/h)
    - Typically 4 AM - 10 AM local time

    Args:
        temp_c: Temperature in Celsius
        dewpoint_c: Dewpoint in Celsius
        wind_speed_kmh: Wind speed in km/h
        hour: Local hour (0-23)

    Returns:
        Penalty factor (0.15 for Tule Fog, 1.0 for no fog)
    """
    spread = temp_c - dewpoint_c

    # Tule Fog detection thresholds
    is_high_humidity = spread < 1.5
    is_calm = wind_speed_kmh < 5.0
    is_fog_hours = 4 <= hour <= 10

    if is_high_humidity and is_calm and is_fog_hours:
        logger.warning(f"[solar_physics] TULE FOG DETECTED: spread={spread:.1f}C, "
                      f"wind={wind_speed_kmh:.1f}km/h, hour={hour}")
        return 0.15  # 85% reduction for dense Tule Fog

    # Moderate fog risk
    if is_high_humidity and is_calm:
        return 0.4  # 60% reduction for potential fog

    if is_high_humidity and is_fog_hours:
        return 0.6  # 40% reduction for humid mornings

    return 1.0  # No penalty


def get_irradiance_category(watts: float) -> str:
    """
    Categorize irradiance level for display.

    Args:
        watts: Solar irradiance in W/m2

    Returns:
        Category string: 'Minimal', 'Low-Moderate', 'Good', 'Peak'
    """
    if watts < 50:
        return "Minimal"
    elif watts < 150:
        return "Low-Moderate"
    elif watts < 400:
        return "Good"
    else:
        return "Peak Production"


if __name__ == "__main__":
    """Test the solar physics module."""
    logging.basicConfig(level=logging.DEBUG)

    print("=" * 60)
    print("Testing Solar Physics Module")
    print("=" * 60)

    # Test theoretical max at different times
    print("\n1. Theoretical Max GHI (Dec 18, clear sky):")
    day_of_year = 352  # December 18
    for hour in [6, 9, 12, 15, 18]:
        max_ghi = calculate_theoretical_max_ghi(hour, day_of_year)
        print(f"   {hour:02d}:00 -> {max_ghi:.0f} W/m2")

    # Test Google-first calculation scenarios
    print("\n2. Solar Calculation Scenarios (Google-first):")

    test_cases = [
        # (om_radiation, google_cloud, hour, description)
        (400, 0, 12, "Google clear - Open-Meteo ignored"),
        (400, 90, 12, "Google sees clouds, Open-Meteo says sunny - Google wins"),
        (100, 0, 12, "Google clear, Open-Meteo low - Google wins"),
        (300, 50, 12, "Moderate clouds per Google"),
        (0, 80, 12, "No Open-Meteo data, Google has clouds"),
        (350, None, 12, "Google MISSING - falls back to Open-Meteo"),
        (0, None, 12, "Neither source - zero"),
    ]

    for om_rad, g_cloud, hour, desc in test_cases:
        result = calculate_hybrid_solar(om_rad, g_cloud, hour, day_of_year)
        print(f"   {desc}:")
        print(f"      OM={om_rad}W, Google={g_cloud}% -> {result:.0f}W")

    # Test Tule Fog detection
    print("\n3. Tule Fog Detection:")
    fog_cases = [
        (5.0, 4.5, 2.0, 7, "Dense Tule Fog conditions"),
        (10.0, 5.0, 3.0, 9, "High humidity but not fog"),
        (15.0, 8.0, 15.0, 10, "Normal conditions"),
    ]

    for temp, dew, wind, hour, desc in fog_cases:
        penalty = calculate_tule_fog_penalty(temp, dew, wind, hour)
        print(f"   {desc}: penalty={penalty:.2f}")

    print("\n" + "=" * 60)
    print("Test complete!")
