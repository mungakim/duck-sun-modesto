"""
Providers package for Duck Sun Modesto: WEIGHTED ENSEMBLE Edition

This package contains data providers that fetch weather data
from multiple sources for the Weighted Ensemble Consensus Model:

1. Open-Meteo - Global ensemble (GFS, ICON, GEM models) - Weight: 1x
2. NOAA - National Oceanic and Atmospheric Administration (official US forecast) - Weight: 3x
3. Met.no - Norwegian Met Institute (ECMWF European model) - Weight: 3x
4. AccuWeather - Commercial provider (5-day forecast) - Weight: 4x
5. Weather.com - The Weather Channel (10-day forecast) - Weight: 4x
6. Weather Underground - IBM/TWC (10-day forecast) - Weight: 4x
7. Google Weather - Google Maps Platform (MetNet-3 neural model) - Weight: 6x
8. MID.org - Modesto Irrigation District local data - Weight: 2x
9. METAR - Real-time airport ground truth observations
RELIABILITY IS KING - Google MetNet-3 neural model leads the weighted ensemble.
"""

from duck_sun.providers.open_meteo import (
    fetch_open_meteo,
    fetch_open_meteo_sync,
    ForecastResult,
    HourlyData,
    MODESTO_LAT,
    MODESTO_LON,
)

from duck_sun.providers.noaa import (
    NOAAProvider,
    NOAATemperature,
)

from duck_sun.providers.met_no import (
    MetNoProvider,
    MetNoTemperature,
)

from duck_sun.providers.accuweather import (
    AccuWeatherProvider,
    AccuWeatherDay,
)

from duck_sun.providers.google_weather import (
    GoogleWeatherProvider,
    GoogleHourlyData,
    GoogleDailyData,
)

from duck_sun.providers.mid_org import (
    MIDOrgProvider,
)

from duck_sun.providers.metar import (
    MetarProvider,
    MetarObservation,
)

from duck_sun.providers.weather_com import (
    WeatherComProvider,
    WeatherComDay,
)

from duck_sun.providers.wunderground import (
    WUndergroundProvider,
    WUndergroundDay,
)

__all__ = [
    # Open-Meteo (fallback source, weight: 1x)
    "fetch_open_meteo",
    "fetch_open_meteo_sync",
    "ForecastResult",
    "HourlyData",
    "MODESTO_LAT",
    "MODESTO_LON",
    # NOAA (US official, weight: 3x)
    "NOAAProvider",
    "NOAATemperature",
    # Met.no (European ECMWF, weight: 3x)
    "MetNoProvider",
    "MetNoTemperature",
    # AccuWeather (commercial, weight: 4x)
    "AccuWeatherProvider",
    "AccuWeatherDay",
    # Google Weather (MetNet-3 neural model, weight: 6x)
    "GoogleWeatherProvider",
    "GoogleHourlyData",
    "GoogleDailyData",
    # MID.org (local Modesto, weight: 2x) - REST API
    "MIDOrgProvider",
    # METAR (ground truth)
    "MetarProvider",
    "MetarObservation",
    # Weather.com (commercial, weight: 4x) - Web scraping
    "WeatherComProvider",
    "WeatherComDay",
    # Weather Underground (commercial, weight: 4x) - Web scraping
    "WUndergroundProvider",
    "WUndergroundDay",
]

