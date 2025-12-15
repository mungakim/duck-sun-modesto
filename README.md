# Duck Sun Modesto

A weighted ensemble solar forecasting engine for Modesto, CA. Triangulates data from 9 weather sources, applies Fog Guard + Smoke Guard physics, and generates a PDF report for Power System Schedulers.

**Status:** Calibrated & Verification Ready (Dec 14, 2025)
**Version:** Uncanny Edition v1.2 (Source Replication)

## How It Works

### Data Flow
```
run_duck_sun.bat (or GitHub Action at 4:55 AM)
         │
         ▼
    python main.py
         │
         ▼
┌────────────────────────────────────┐
│  Fetch from 9 Weather Sources      │
│  (concurrent async requests)       │
└────────────────────────────────────┘
         │
    ┌────┴────┬────────┬────────┬────────┬────────┬────────┬────────┐
    ▼         ▼        ▼        ▼        ▼        ▼        ▼        ▼
 Open-     HRRR      NWS    Met.no   Accu    Weather  MID.org  Smoke
 Meteo    (3km)    (govt)  (ECMWF)  Weather   .com    (local)   AQI
(fresh)  (1hr     (fresh) (fresh)  (42/day (6hr    (cached)  (fresh)
         cache)                    limit)   cache)
         │
         ▼
┌────────────────────────────────────┐
│  Weighted Ensemble Consensus       │
│  NWS(5x) > Accu(3x) > WC(2x) > OM  │
│  + Fog Guard + Smoke Guard         │
└────────────────────────────────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
  JSON      PDF         Console
 Output    Report       Summary
```

### Data Sources & Caching

| Source | Rate Limit | Cache Strategy | Notes |
|--------|-----------|----------------|-------|
| Open-Meteo | Unlimited | Fresh every run | Primary model (GFS/ICON/GEM) |
| HRRR | Unlimited | 1 hour cache | High-res 3km, 15-min updates |
| NWS | Unlimited | Fresh every run | US govt official forecast |
| Met.no | Unlimited | Fresh every run | European ECMWF model |
| AccuWeather | **50/day** | **42 calls/day then locks** | After 42nd call, uses cache until midnight |
| Weather.com | N/A | 6 hours | JS-rendered, manual cache |
| MID.org | Unknown | Cached | Local Modesto microclimate |
| METAR | Unlimited | Fresh every run | KMOD airport ground truth |
| Smoke/AQI | Unlimited | Fresh every run | PM2.5 wildfire detection |

**Fresh Data Policy:** Every run fetches fresh data from unlimited sources. AccuWeather allows up to 42 calls/day (safety margin under 50 limit), then locks to cached data until midnight reset.

### Source Replication (Dec 14, 2025)

The system uses **Source Replication** rather than Model Approximation:

| Provider | API Endpoint | Alignment |
|----------|-------------|-----------|
| **NWS** | `/forecast` (Period API) | Matches weather.gov exactly |
| **AccuWeather** | Official 5-day API | Matches accuweather.com |
| **Weather.com** | Manual ground truth | Matches weather.com 10-day |
| **Open-Meteo** | Hourly physics model | Independent (Duck Curve) |

**Key Insight:** NWS uses the human-curated Period forecast (`/forecast`) instead of raw hourly gridpoint data (`/gridpoints`). This ensures alignment with the official NWS website without any hardcoding.

## Usage

### Manual Run (Windows)
Double-click `run_duck_sun.bat` - fetches fresh data and generates PDF.

### Command Line
```bash
python main.py
```

### Outputs
- `reports/YYYY-MM/YYYY-MM-DD/daily_forecast_*.pdf` - Power System Scheduler report (organized by date)
- `outputs/solar_data_YYYY-MM-DD_HH-MM-SS.json` - Raw consensus data
- `LEADERBOARD.md` - 10-day accuracy rankings

## Automated Scheduling

### GitHub Actions (Primary)
Runs daily at **4:55 AM Pacific** with DST handling:
```yaml
schedule:
  - cron: '55 12 * * *'  # 4:55 AM PST (winter)
  - cron: '55 11 * * *'  # 4:55 AM PDT (summer)
```

PDF is committed to repo and available by ~5:00 AM for the scheduler.

### Windows Task Scheduler (Backup)
Use `run_duck_sun.bat` for local automated runs.

## PDF Report Features

- **8-Day Temperature Grid**: 4 sources (Open-Meteo, NWS, Weather.com, AccuWeather) + weighted consensus
- **MID GAS BURN**: 3 blank cells for manual entry (date | MMBtu)
- **PGE CITYGATE**: Blank cell for price entry
- **MID WEATHER 48-Hour Summary**: Color-coded High (orange) / Low (blue) cells with historical records
- **Precipitation Consensus**: Rain probability from NOAA HRRR (3km), Open-Meteo, Weather.com, AccuWeather
- **3-Day Solar Forecast (HE09-HE16)**: Hourly W/m² irradiance with condition descriptions
- **Solar Irradiance Legend**: Color-coded production levels (<50 Minimal, 50-150 Low-Moderate, 150-400 Good, >400 Peak)

Manual fields can be filled in with pen after printing, or add text boxes in Acrobat before saving/emailing.

## Key Concepts

- **Solar Factor (0-1)**: Normalized solar production potential
- **Duck Curve Hours (HE09-HE16)**: 9 AM - 4 PM when solar ramps dramatically
- **Tule Fog**: Dense ground fog (dewpoint depression < 2.5°C, wind < 8 km/h)
- **Smoke Shade**: PM2.5 > 35 µg/m³ reduces solar output

### Fog Guard Physics

The system models Central Valley fog dynamics with **pre-dawn lock-in detection**:

- **24/7 Monitoring**: Fog probability calculated continuously, not just during sun hours
- **Pre-Dawn Lock (4-8 AM)**: If fog probability exceeds 80% during this window, an inversion layer has formed
- **Persistent Stratus**: Once locked in, fog persists until afternoon solar heating breaks the inversion

**Three-Tier Risk Assessment:**
| Risk Level | Condition | Solar Penalty |
|------------|-----------|---------------|
| CRITICAL | Active fog (saturated + stagnant) | 85% |
| HIGH | Persistent stratus (locked in from pre-dawn) | 60% |
| MODERATE | Elevated fog probability (>50%) | 30% |

The "Modesto Bowl" topography traps cold air, creating inversions that standard weather models miss.

## Installation

```bash
pip install -r requirements.txt
```

Required: `ACCUWEATHER_API_KEY` in `.env` (optional but recommended for commercial accuracy).
