# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Duck Sun Modesto is a daily solar forecasting agent for Modesto, CA power system scheduling. It fetches weather data from 9 sources, computes deterministic solar factors, and generates PDF reports for Power System Schedulers.

**Current Status:** Production Ready - Google MetNet-3 Neural Model Integration (Dec 18, 2025)

## Architecture

The project follows a **Source Replication** approach (not Model Approximation):
- **providers/** - Data fetching with organic API sourcing (matches official websites)
- **scheduler.py** - Orchestration for the daily workflow (fetch data → save JSON → generate PDF)
- **pdf_report.py** - ReportLab-based PDF generation for Power System Schedulers

### Key Design Principles

1. **Source Replication:** Each provider fetches from the exact same API endpoint that powers the official website, ensuring organic alignment without hardcoding.
2. **Deterministic Solar Math:** Solar factor calculation is done in Python for 100% accuracy.
3. **Weighted Ensemble:** Google(6x) > AccuWeather(4x) > NOAA(3x) > Met.no(3x) > Open-Meteo(1x)

### Data Sourcing Strategy

| Provider | API Endpoint | Alignment Target | Weight |
|----------|-------------|------------------|--------|
| **Google Weather** | Maps Platform Weather API (MetNet-3) | Neural/satellite fusion | **6x** |
| **AccuWeather** | Official 5-day API | accuweather.com | 4x |
| **NOAA** | `/gridpoints/{wfo}/{x},{y}/forecast` (Periods) | weather.gov website | 3x |
| **Met.no** | Locationforecast 2.0 API (ECMWF) | Norwegian Met Institute | 3x |
| **Open-Meteo** | Hourly GFS/ICON/GEM models | Physics-based (independent) | 1x |

**Google Weather (MetNet-3):** The primary source uses Google's neural weather model which fuses satellite imagery and radar data for hyperlocal 0-96 hour predictions. Superior short-term accuracy compared to pure physics models.

**NOAA Organic Sourcing:** The NOAA provider uses the `/forecast` endpoint (human-curated Period data) rather than `/gridpoints` hourly model data. This ensures the PDF temperatures match the official weather.gov website exactly.

## Commands

**CRITICAL: WSL/Windows Python Environment**

This project runs on Windows filesystem (`/mnt/c/...`) accessed via WSL. The virtual environment was created with Windows Python, so you MUST use the Windows Python executable directly. **Do NOT try to `source activate`** - it won't work.

```bash
# CORRECT - Use Windows Python executable directly
./venv/Scripts/python.exe -m duck_sun.scheduler

# WRONG - These will all fail in WSL:
# python -m duck_sun.scheduler          # "command not found"
# python3 -m duck_sun.scheduler         # uses system Python, missing deps
# source venv/bin/activate              # path doesn't exist (Windows venv)
# source venv/Scripts/activate          # activates but python still not found
```

**All Commands (use `./venv/Scripts/python.exe`):**

```bash
# Run the full daily workflow (fetch data + generate PDF)
./venv/Scripts/python.exe -m duck_sun.scheduler

# Test the NOAA provider directly
./venv/Scripts/python.exe -m duck_sun.providers.noaa

# Test the Open-Meteo provider directly
./venv/Scripts/python.exe -m duck_sun.providers.open_meteo

# Install dependencies (use Windows pip)
./venv/Scripts/pip.exe install -r requirements.txt
```

## Environment Variables

Required in `.env`:
- `GOOGLE_MAPS_API_KEY` - Google Maps Platform Weather API key (MetNet-3 neural model)
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
- 8-day temperature grid from 5 sources with weighted consensus
- MID Weather 48-hour summary with historical records
- Precipitation % from ensemble (NOAA HRRR, Open-Meteo, AccuWeather, Google)
- 3-day solar forecast (HE09-HE16) with hourly W/m² and condition descriptions
- Solar irradiance legend: <50 Minimal, 50-150 Low-Moderate, 150-400 Good, >400 Peak Production

## Calibration Status (Dec 18, 2025)

**Google MetNet-3 Integration Complete:**
- **Google Weather:** MetNet-3 neural model via Maps Platform Weather API - Weight: 6x
- **AccuWeather:** Direct API sourcing (matches accuweather.com) - Weight: 4x
- **NOAA:** Organic alignment via `/forecast` Period API (matches weather.gov) - Weight: 3x
- **Met.no:** ECMWF European model via Locationforecast 2.0 API - Weight: 3x
- **Open-Meteo:** Independent physics model (provides "second opinion") - Weight: 1x

**Weight Rationale:**
- Google MetNet-3 uses real-time radar/satellite fusion for superior 0-96 hour accuracy
- Neural model "nowcasts" rather than just physics simulations
- Best for hyperlocal, short-term predictions (ideal for duck curve forecasting)

**Previous Verification Results (Dec 16 Test Case):**
- Actual: High 51°F, Low 41°F
- **AccuWeather:** Predicted 48-51°F → Winner (0-3°F error)
- **NOAA:** Predicted 58°F → Miss (+7°F)
- **Open-Meteo:** Predicted 60°F → Major miss (+9°F)
