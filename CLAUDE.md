# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Duck Sun Modesto is a daily solar forecasting agent for Modesto, CA power system scheduling. It fetches weather data from 8 sources, computes deterministic solar factors, and generates PDF reports for Power System Schedulers.

**Current Status:** Production Ready - Calibrated for 7-Day Verification Test (Dec 16, 2025)

## Architecture

The project follows a **Source Replication** approach (not Model Approximation):
- **providers/** - Data fetching with organic API sourcing (matches official websites)
- **scheduler.py** - Orchestration for the daily workflow (fetch data → save JSON → generate PDF)
- **pdf_report.py** - ReportLab-based PDF generation for Power System Schedulers

### Key Design Principles

1. **Source Replication:** Each provider fetches from the exact same API endpoint that powers the official website, ensuring organic alignment without hardcoding.
2. **Deterministic Solar Math:** Solar factor calculation is done in Python for 100% accuracy.
3. **Weighted Ensemble:** AccuWeather(10x) > NOAA(3x) > Met.no(3x) > Open-Meteo(1x)

### Data Sourcing Strategy

| Provider | API Endpoint | Alignment Target |
|----------|-------------|------------------|
| **NOAA** | `/gridpoints/{wfo}/{x},{y}/forecast` (Periods) | weather.gov website |
| **AccuWeather** | Official 5-day API | accuweather.com |
| **Met.no** | Locationforecast 2.0 API (ECMWF) | Norwegian Met Institute |
| **Open-Meteo** | Hourly GFS/ICON/GEM models | Physics-based (independent) |

**NOAA Organic Sourcing:** The NOAA provider uses the `/forecast` endpoint (human-curated Period data) rather than `/gridpoints` hourly model data. This ensures the PDF temperatures match the official weather.gov website exactly.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full daily workflow (fetch data + generate briefing)
python -m duck_sun.scheduler

# Test the NOAA provider directly
python -m duck_sun.providers.noaa

# Test the Open-Meteo provider directly
python -m duck_sun.providers.open_meteo
```

## Environment Variables

Required in `.env`:
- `ACCUWEATHER_API_KEY` - AccuWeather API key for forecast data
- `LOG_LEVEL` (optional) - Defaults to INFO

## Key Concepts

- **Solar Factor (0-1)**: Normalized solar production potential. Calculated as `(radiation/900) * (1 - 0.7 * cloud_penalty)`
- **Duck Curve Hours (HE09-HE16)**: Critical period when solar ramps up dramatically (9 AM to 4 PM local time)
- **MAX_GHI**: 900 W/m² maximum expected Global Horizontal Irradiance for the region

## Output Files

- `outputs/solar_data_YYYY-MM-DD_HH-MM-SS.json` - Raw solar metrics and consensus data
- `reports/YYYY-MM/YYYY-MM-DD/daily_forecast_*.pdf` - PDF one-pager for Power System Schedulers (organized by date)

## PDF Report Structure

The PDF report includes:
- 8-day temperature grid from 4 sources with weighted consensus
- MID Weather 48-hour summary with historical records
- Precipitation % from ensemble (NOAA HRRR, Open-Meteo, AccuWeather)
- 3-day solar forecast (HE09-HE16) with hourly W/m² and condition descriptions
- Solar irradiance legend: <50 Minimal, 50-150 Low-Moderate, 150-400 Good, >400 Peak Production

## Calibration Status (Dec 16, 2025)

**Source Replication Complete:**
- **NOAA:** Organic alignment via `/forecast` Period API (matches weather.gov)
- **AccuWeather:** Direct API sourcing (matches accuweather.com)
- **Met.no:** ECMWF European model via Locationforecast 2.0 API
- **Open-Meteo:** Independent physics model (provides "second opinion")

**Verification Results (Dec 16 Test Case):**
- Actual: High 51°F, Low 41°F
- **AccuWeather:** Predicted 48-51°F → **Winner** (0-3°F error)
- **NOAA:** Predicted 58°F → Miss (+7°F)
- **Met.no:** ECMWF European model (new addition)
- **Open-Meteo:** Predicted 60°F → Major miss (+9°F)

**Weight Adjustment:** AccuWeather promoted to 10x (highest), NOAA and Met.no at 3x, Open-Meteo at 1x based on 2-day forecast accuracy.
