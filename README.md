# Duck Sun Modesto (Silent Edition)

A physics-based solar forecasting engine for Modesto, CA. It triangulates data from 3 global weather models, applies local "Fog Guard" physics, and generates a condensed PDF report for grid schedulers.

## Prerequisites

- Python 3.9+
- No API keys required (All data sources are open/free)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Generate the daily forecast PDF:

```bash
python -m duck_sun.main
```

Or run via the scheduler module:

```bash
python -m duck_sun.scheduler
```

This creates:
- `outputs/solar_data_YYYY-MM-DD_HH-MM-SS.json` - Raw consensus data
- `reports/daily_forecast_YYYY-MM-DD_HH-MM-SS.pdf` - The "Gold Bar" PDF report

## Key Components

- **Uncanny Engine**: Physics model that detects inversion layers and fog lock-in.
- **Truth Tracker**: SQLite database that logs predictions and verifies them 24h later.
- **PDF Generator**: Creates the "Gold Bar" report for grid ops.
- **Fog Guard**: Tule Fog detection using dewpoint depression and wind stagnation.
- **Smoke Guard**: PM2.5 analysis for wildfire smoke impact on solar.

## Data Sources (All Free/Open)

1. **Open-Meteo** - Global ensemble (GFS/ICON/GEM models)
2. **NWS** - US National Weather Service official forecast
3. **Met.no** - Norwegian Meteorological Institute (ECMWF model)
4. **METAR** - KMOD Airport ground truth observations
5. **Air Quality API** - PM2.5/smoke detection

## Example Output

```
============================================================
   DUCK SUN MODESTO: SILENT EDITION
   Consensus Temperature Triangulation System
   + Fog Guard + Smoke Guard
============================================================
   [SOURCES] Open-Meteo + NWS + Met.no + METAR + AQI
   [MODE] Pure Deterministic (No AI/LLM)

STEP 1: Fetching Weather Data
----------------------------------------
[1/5] Polling Open-Meteo (GFS/ICON/GEM)...
      OK - 192 hourly records
...

SUCCESS!
   Raw data: outputs/solar_data_2025-12-12_05-23-00.json
   PDF: reports/daily_forecast_2025-12-12_05-23-00.pdf
   Duration: 8.42 seconds
```

## Automated Scheduling

The project includes a GitHub Actions workflow that runs daily at 5:23 AM PST:

```yaml
# .github/workflows/daily_forecast.yml
on:
  schedule:
    - cron: '23 13 * * *'  # 5:23 AM PST = 13:23 UTC
```

For local scheduling, use Task Scheduler (Windows) or cron (Linux/Mac):

```bash
# Linux/Mac crontab
23 5 * * * cd /path/to/duck-sun-modesto && python -m duck_sun.main >> cron.log 2>&1
```

## Key Concepts

- **Solar Factor (0-1)**: Normalized solar production potential. 1.0 = perfect conditions, 0.0 = no production.
- **Duck Curve Hours (HE09-HE16)**: 9 AM to 4 PM local time, when solar ramps dramatically.
- **Tule Fog**: Dense ground fog that forms when dewpoint depression < 2.5°C and wind < 8 km/h.
- **Smoke Shade**: Wildfire smoke reduces solar differently than clouds (steady reduction vs. intermittent).
