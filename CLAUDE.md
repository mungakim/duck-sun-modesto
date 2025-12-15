# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Duck Sun Modesto is a daily solar forecasting agent for Modesto, CA grid scheduling. It fetches weather data, computes deterministic solar factors, and generates human-readable briefings using Claude.

## Architecture

The project follows a deterministic-first approach:
- **providers/** - Data fetching with deterministic Python solar calculations (no LLM involved)
- **agent.py** - Claude SDK integration for interpreting data and generating briefings
- **scheduler.py** - Orchestration for the daily workflow (fetch data → save JSON → generate report)

Key design principle: Solar math (solar_factor calculation) is done in Python for 100% accuracy. Claude only handles interpretation and narrative generation.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full daily workflow (fetch data + generate briefing)
python -m duck_sun.scheduler

# Test the agent directly (generates briefing only)
python -m duck_sun.agent

# Test the Open-Meteo provider directly
python -m duck_sun.providers.open_meteo
```

## Environment Variables

Required in `.env`:
- `ANTHROPIC_API_KEY` - Claude API key for briefing generation
- `LOG_LEVEL` (optional) - Defaults to INFO

## Key Concepts

- **Solar Factor (0-1)**: Normalized solar production potential. Calculated as `(radiation/900) * (1 - 0.7 * cloud_penalty)`
- **Duck Curve Hours (HE09-HE16)**: Critical period when solar ramps up dramatically (9 AM to 4 PM local time)
- **MAX_GHI**: 900 W/m² maximum expected Global Horizontal Irradiance for the region

## Output Files

- `outputs/solar_data_YYYY-MM-DD_HH-MM-SS.json` - Raw solar metrics and consensus data
- `reports/daily_forecast_YYYY-MM-DD_HH-MM-SS.pdf` - PDF one-pager for grid schedulers

## PDF Report Structure

The PDF report includes:
- 8-day temperature grid from 4 sources with weighted consensus
- MID Weather 48-hour summary with historical records
- Precipitation % from ensemble (NOAA HRRR, Open-Meteo, Weather.com, AccuWeather)
- 3-day solar forecast (HE09-HE16) with hourly W/m² and condition descriptions
- Solar irradiance legend: <50 Minimal, 50-150 Low-Moderate, 150-400 Good, >400 Peak Production
