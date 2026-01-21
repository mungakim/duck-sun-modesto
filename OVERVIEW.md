# Duck Sun Modesto - System Overview

## What It Does

Duck Sun Modesto is an automated solar forecasting system for Modesto, CA power system scheduling. It generates daily forecast reports that help Power System Schedulers anticipate solar generation output during critical "duck curve" hours (9 AM - 4 PM).

**Key Output:** A daily Excel report with 8-day temperature forecasts, precipitation probabilities, and solar irradiance predictions.

---

## How It Works

### Data Collection (11 Weather Sources)

The system fetches weather data from multiple providers and combines them using weighted ensemble averaging:

| Provider | Method | Weight | Purpose |
|----------|--------|--------|---------|
| **Google Weather** | API (MetNet-3 neural model) | 6x | Primary source - satellite/radar fusion |
| **AccuWeather** | API | 4x | Commercial provider |
| **Weather.com** | Web scraping (rate limited) | 4x | IBM/TWC data |
| **Weather Underground** | Web scraping (rate limited) | 4x | IBM/TWC data |
| **NOAA** | API (weather.gov) | 3x | US government source |
| **Met.no** | API (ECMWF model) | 3x | European physics model |
| **Open-Meteo** | API (GFS/ICON/GEM) | 1x | Baseline/fallback |
| **MID.org** | API | 2x | Local Modesto microclimate |
| **HRRR** | API | - | High-resolution precipitation |
| **METAR** | API | - | Airport ground truth |
| **Smoke/AQI** | API | - | Wildfire smoke detection |

### Processing Pipeline

```
1. FETCH DATA        2. VALIDATE           3. PHYSICS ENGINE      4. GENERATE REPORT
   (11 providers)       (retry failures)      (solar calculations)   (Excel to network)
        |                    |                      |                      |
        v                    v                      v                      v
   [API calls]  -->  [Check completeness]  -->  [Weighted        -->  [Save to X: drive]
   [Web scrapes]     [Retry 3x if needed]       consensus]            [Save locally]
   [Cache fallback]                             [Duck curve]
                                                [Fog detection]
```

### Key Features

1. **Weighted Ensemble Consensus**
   - Higher-weight sources (Google, AccuWeather) influence the final temperature more
   - "Google Veto" rule: Demotes Google's weight if it deviates >10°F from peers

2. **Resilience & Caching**
   - Every provider has Last Known Good (LKG) cache
   - Report NEVER shows "--" - always falls back to cached or default data
   - Tiered staleness: FRESH (<10min) → ACCEPTABLE (<6hr) → STALE (<24hr) → DEFAULT

3. **Rate Limiting (Web Scraping)**
   - Weather.com and Weather Underground limited to 3 calls/day each
   - Returns cached data when limit reached

4. **Fallback Synthesis**
   - If Open-Meteo (baseline) fails, synthesizes data from Google → AccuWeather → NOAA → Met.no

---

## Directory Structure

```
duck_sun/
├── scheduler.py          # Main orchestrator - runs the daily workflow
├── providers/            # Data fetching (11 provider modules)
│   ├── google_weather.py
│   ├── accuweather.py
│   ├── weather_com.py    # Web scraping, 3/day limit
│   ├── wunderground.py   # Web scraping, 3/day limit
│   ├── noaa.py
│   ├── met_no.py
│   ├── open_meteo.py
│   ├── mid_org.py
│   ├── metar.py
│   └── smoke.py
├── uncanniness.py        # Physics engine (solar math, fog detection)
├── ensemble.py           # Weighted consensus calculations
├── solar_physics.py      # Solar factor & irradiance math
├── cache_manager.py      # LKG cache with tiered staleness
├── resilience.py         # Retry logic & error handling
├── verification.py       # Truth Tracker (accuracy scoring)
├── excel_report.py       # Report generation
└── pdf_report.py         # Alternative PDF output
```

---

## Environment Variables

Required in `.env`:

```
GOOGLE_MAPS_API_KEY=your_key_here
ACCUWEATHER_API_KEY=your_key_here
TWC_API_KEY=your_key_here
```

Optional:
```
LOG_LEVEL=INFO                # DEBUG for verbose output
DUCK_SUN_SKIP_SSL_VERIFY=1    # Required for MID corporate proxy
```

---

## Running the System

```bash
# Run forecast (from WSL)
./venv/Scripts/python.exe -m duck_sun.scheduler

# Run forecast + commit + push to GitHub
./run_and_push.sh
```

---

## Output Files

| Location | File | Purpose |
|----------|------|---------|
| `reports/YYYY-MM/YYYY-MM-DD/` | `daily_forecast_*.xlsx` | Local Excel report |
| `X:\Operatns\Pwrsched\Weather\YYYY-MM\YYYY-MM-DD\` | `daily_forecast_*.xlsx` | Network share for team |
| `outputs/` | `solar_data_*.json` | Raw JSON data |
| `outputs/cache/` | `*_lkg.json` | Provider cache files |
| `logs/` | `duck_sun.log` | Execution logs |

---

## Report Contents

The Excel report includes:

1. **8-Day Temperature Grid** - High/Low from 7 sources + weighted consensus
2. **MID Weather Summary** - Local 48-hour with historical records
3. **Precipitation Forecast** - % chance from ensemble
4. **3-Day Solar Forecast** - Hourly W/m² for duck curve hours (HE09-HE16)
5. **Fog/Smoke Alerts** - Tule fog detection, wildfire smoke warnings

---

## Verification System

The Truth Tracker (`verification.db`) scores provider accuracy:

- Logs each provider's forecast
- Fetches actual weather from Open-Meteo Historical API
- Calculates Mean Absolute Error (MAE) per source
- Generates accuracy leaderboard

Run standalone: `./venv/Scripts/python.exe -m duck_sun.verification`

---

## Security Notes

- All API keys loaded from environment variables (never hardcoded)
- HTTPS connections configured to trust MID corporate SSL proxy inspection certificates
- Web scraping rate-limited to 3 calls/day per source to minimize external footprint
- API error responses truncated in logs to limit data exposure
