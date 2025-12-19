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


def calculate_hybrid_solar(
    om_radiation: float,
    google_cloud: int,
    hour: int,
    day_of_year: int
) -> float:
    """
    Calculate solar irradiance using Hybrid Logic.

    Combines:
    - Open-Meteo physics model (radiative transfer calculations)
    - Google MetNet-3 neural model (precise cloud timing from satellite/radar)

    Args:
        om_radiation: Watts/m2 from Open-Meteo (Physics baseline)
        google_cloud: Cloud cover percentage from Google (0-100)
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)

    Returns:
        Hybrid solar irradiance in W/m2
    """
    # 1. Theoretical Max (Clear Sky GHI) for Modesto
    max_theoretical = calculate_theoretical_max_ghi(hour, day_of_year)

    if max_theoretical <= 0:
        # Sun is down - no solar production
        return 0.0

    # 2. Physics Baseline (Trust Open-Meteo's radiative transfer model first)
    # If OM is missing/zero, fallback to theoretical with cloud penalty
    if om_radiation > 0:
        base_solar = om_radiation
    else:
        # Fallback: theoretical max with simple cloud attenuation
        cloud_fraction = google_cloud / 100.0
        attenuation = 1.0 - (0.7 * cloud_fraction)
        base_solar = max_theoretical * attenuation

    # 3. The "Google Veto" (Timing Correction)
    # If Google says it's "Heavy Cloud" (>80%) but Physics model says "Sunny" (>200W),
    # Trust Google's timing and clamp it down.
    # Google's satellite/radar fusion is better at timing than physics models.
    if google_cloud > 80 and base_solar > 200:
        # It's likely a timing mismatch. The cloud IS there (per Google).
        # Clamp to diffuse-only levels (~30% of base)
        clamped_solar = base_solar * 0.3
        logger.debug(f"[solar_physics] GOOGLE VETO: cloud={google_cloud}%, "
                    f"base={base_solar:.0f}W -> clamped={clamped_solar:.0f}W")
        return clamped_solar

    # 4. The "Clear Sky" Boost
    # If Google says 0-10% clouds, trust the higher of the two values
    # (physics model may underestimate on truly clear days)
    if google_cloud < 10:
        boosted = max(base_solar, max_theoretical * 0.9)
        if boosted > base_solar:
            logger.debug(f"[solar_physics] CLEAR SKY BOOST: {base_solar:.0f}W -> {boosted:.0f}W")
        return boosted

    # 5. Moderate cloud adjustment (10-80%)
    # Blend physics and AI-adjusted values
    if 10 <= google_cloud <= 80:
        # Apply partial cloud attenuation based on Google's cloud cover
        cloud_factor = 1.0 - (0.5 * (google_cloud / 100.0))
        blended = base_solar * cloud_factor
        return max(blended, base_solar * 0.3)  # Never go below diffuse minimum

    return base_solar


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

    # Test hybrid calculation scenarios
    print("\n2. Hybrid Solar Calculation Scenarios:")

    test_cases = [
        # (om_radiation, google_cloud, hour, description)
        (400, 0, 12, "Clear noon (both agree)"),
        (400, 90, 12, "Google sees clouds, physics says sunny"),
        (100, 0, 12, "Google clear, physics low"),
        (300, 50, 12, "Moderate clouds"),
        (0, 80, 12, "No physics data, high clouds"),
    ]

    for om_rad, g_cloud, hour, desc in test_cases:
        result = calculate_hybrid_solar(om_rad, g_cloud, hour, day_of_year)
        print(f"   {desc}:")
        print(f"      OM={om_rad}W, Google={g_cloud}% -> Hybrid={result:.0f}W")

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
