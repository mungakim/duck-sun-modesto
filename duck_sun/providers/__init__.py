"""
Providers package for Duck Sun Modesto: Uncanny Edition

This package contains data providers that fetch weather data
from multiple sources for the Consensus Temperature Model:

1. Open-Meteo - Global ensemble (GFS, ICON, GEM models)
2. NWS - National Weather Service (official US forecast)
3. Met.no - Norwegian Met Institute (ECMWF European model)
4. METAR - Real-time airport ground truth observations
5. Smoke - Open-Meteo Air Quality API (PM2.5/AQI for wildfire smoke)
"""

from duck_sun.providers.open_meteo import (
    fetch_open_meteo,
    fetch_open_meteo_sync,
    ForecastResult,
    HourlyData,
    MODESTO_LAT,
    MODESTO_LON,
)

from duck_sun.providers.nws import (
    NWSProvider,
    NWSTemperature,
)

from duck_sun.providers.met_no import (
    MetNoProvider,
    MetNoTemperature,
)

from duck_sun.providers.metar import (
    MetarProvider,
    MetarObservation,
)

from duck_sun.providers.smoke import (
    SmokeProvider,
    SmokeMetrics,
)

__all__ = [
    # Open-Meteo (primary source)
    "fetch_open_meteo",
    "fetch_open_meteo_sync",
    "ForecastResult",
    "HourlyData",
    "MODESTO_LAT",
    "MODESTO_LON",
    # NWS (US official)
    "NWSProvider",
    "NWSTemperature",
    # Met.no (European ECMWF)
    "MetNoProvider",
    "MetNoTemperature",
    # METAR (ground truth)
    "MetarProvider",
    "MetarObservation",
    # Smoke (air quality/wildfire)
    "SmokeProvider",
    "SmokeMetrics",
]

