# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Duck Sun Modesto is a daily solar forecasting agent for Modesto, CA power system scheduling. It fetches weather data from 11 sources, computes deterministic solar factors, and generates PDF reports for Power System Schedulers.

**Current Status:** Production Ready - 11-Source Weighted Ensemble (Jan 15, 2026)

## Architecture

The project follows a **Source Replication** approach (not Model Approximation):
- **providers/** - Data fetching with organic API sourcing (matches official websites)
- **scheduler.py** - Orchestration for the daily workflow (fetch data → save JSON → generate PDF)
- **pdf_report.py** - ReportLab-based PDF generation for Power System Schedulers

### Key Design Principles

1. **Source Replication:** Each provider fetches from the exact same API endpoint that powers the official website, ensuring organic alignment without hardcoding.
2. **Deterministic Solar Math:** Solar factor calculation is done in Python for 100% accuracy.
3. **Weighted Ensemble:** Google(8x) > AccuWeather(4x) = Weather.com(4x) = WUnderground(4x) > NOAA(3x) > Open-Meteo(1x)

### Data Sourcing Strategy

| Provider | API Endpoint | Alignment Target | Weight |
|----------|-------------|------------------|--------|
| **Google Weather** | Maps Platform Weather API (MetNet-3) | Neural/satellite fusion | **8x** |
| **AccuWeather** | Official 5-day API | accuweather.com | 4x |
| **Weather.com** | Web scraping (curl_cffi) | weather.com | 4x |
| **Weather Underground** | Web scraping (curl_cffi) | wunderground.com | 4x |
| **NOAA** | `/gridpoints/{wfo}/{x},{y}/forecast` (Periods) | weather.gov website | 3x |
| **Open-Meteo** | Hourly GFS/ICON/GEM models | Physics-based (independent) | 1x |

**Google Weather (MetNet-3):** The primary source uses Google's neural weather model which fuses satellite imagery and radar data for hyperlocal predictions. Superior short-term accuracy compared to pure physics models.

The provider pulls the **full 240-hour (10-day) window**, which is the documented ceiling on `forecast/hours:lookup` (`hours` = 1..240, `pageSize` = 1..24, so a full pull is 10 paginated calls). Forecast length is **not** gated by billing tier - the Weather API is a single per-call SKU, so a cheaper plan limits call volume, not horizon. Before Jul 2026 the report only showed 4 days of Google data purely because the provider requested `hours=96`.

**Weather.com & Weather Underground:** Both sources are scraped using curl_cffi with browser impersonation. They share data from The Weather Company (IBM) but may show slight variations. Note: Weather.com has aggressive anti-bot protection and may not work in all environments (cloud/container IPs are often blocked).

**NOAA Organic Sourcing:** The NOAA provider uses the `/forecast` endpoint (human-curated Period data) rather than `/gridpoints` hourly model data. This ensures the PDF temperatures match the official weather.gov website exactly.

## Commands

### Default Workflow: Run and Push

The primary command runs the forecast, commits outputs, and pushes to GitHub in one step:

```bash
# DEFAULT - Run forecast + commit + push (recommended)
./run_and_push.sh          # From WSL/Bash
.\run_and_push.ps1         # From Windows PowerShell
```

This script:
1. Runs `./venv/Scripts/python.exe -m duck_sun.scheduler`
2. Stages `outputs/` and `reports/` folders
3. Commits with message "Forecast: YYYY-MM-DD"
4. Pushes to GitHub

### Other Commands

```bash
# Run forecast only (no commit/push)
./venv/Scripts/python.exe -m duck_sun.scheduler

# Test individual providers
./venv/Scripts/python.exe -m duck_sun.providers.noaa
./venv/Scripts/python.exe -m duck_sun.providers.open_meteo

# Install dependencies
./venv/Scripts/pip.exe install -r requirements.txt
```

### Shipping a New Version (Code Deploy to X:\)

The daily forecast runs on the X:\ network drive via a signed PyInstaller exe. When you change code (not just outputs), rebuild and redeploy:

```powershell
.\deploy.ps1               # push code to GitHub + build + sign + copy exe to X:\
```

Equivalent manual sequence:
```powershell
git push origin <branch>   # publish code changes
.\build_exe.ps1            # PyInstaller + Authenticode sign + copy to X:\
```

The dev machine venv lives in `./venv` (Windows Python). PyInstaller bundles that interpreter, so whatever Python version the venv targets is what the production exe runs.

### Python 3.14 + truststore note

`truststore` 0.10.4 has a known recursion bug against Python 3.14's new `SSLContext.verify_mode` setter (RecursionError on every HTTPS call). `duck_sun/ssl_helper.py` detects Python 3.14+ and automatically falls back to stdlib SSL + manual Windows cert loading, which works fine on the corporate network. No user action needed. Once truststore ships a fixed release, the guard in `ssl_helper.py` can be relaxed.

### WSL/Windows Python Environment

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
- 8-day temperature grid from 6 sources with weighted consensus (all 6 sources now cover the full 8 days)
- MID Weather 48-hour summary with historical records
- Precipitation % from ensemble (NOAA HRRR, Open-Meteo, AccuWeather, Google)
- Portland, OR side-reference row (single Hi/Lo line, Google Weather, excluded from the Modesto consensus)
- 7-day solar forecast (HE09-HE16) with hourly W/m² and condition descriptions, 100% Google MetNet-3
- Solar irradiance legend: <50 Minimal, 50-150 Low-Moderate, 150-400 Good, >400 Peak Production

## Calibration Status (Jan 15, 2026)

**11-Source Weighted Ensemble:**
- **Google Weather:** MetNet-3 neural model via Maps Platform Weather API - Weight: 8x
- **AccuWeather:** Direct API sourcing (matches accuweather.com) - Weight: 4x
- **Weather.com:** Web scraping via curl_cffi - Weight: 4x
- **Weather Underground:** Web scraping via curl_cffi - Weight: 4x
- **NOAA:** Organic alignment via `/forecast` Period API (matches weather.gov) - Weight: 3x
- **Open-Meteo:** Independent physics model (provides "second opinion") - Weight: 1x

**Weight Rationale:**
- Google MetNet-3 uses real-time radar/satellite fusion for superior short-range accuracy across its 240-hour window
- Weather.com and Weather Underground (both IBM/TWC) provide additional commercial-grade forecasts
- Neural model "nowcasts" rather than just physics simulations
- Best for hyperlocal, short-term predictions (ideal for duck curve forecasting)

**Previous Verification Results (Dec 16 Test Case):**
- Actual: High 51°F, Low 41°F
- **AccuWeather:** Predicted 48-51°F → Winner (0-3°F error)
- **NOAA:** Predicted 58°F → Miss (+7°F)
- **Open-Meteo:** Predicted 60°F → Major miss (+9°F)

## Data Freshness & Cache Reliability

**CRITICAL: Weather.com temps MUST match the weather.com website.** Any drift of 3-4°F indicates stale cached data being served instead of a fresh API call. This has happened before (Feb 2026) and must not recur.

### Root Cause of Stale Data (Feb 2026 Incident)
1. MID firewall blocked some HTTPS connections (there is NO corporate proxy — confirmed by IT Security)
2. Provider requests failed → returned None → CacheManager served days-old LKG data
3. No maximum cache age was enforced, so 3-day-old forecasts were silently used

### Safeguards Now In Place

**Provider-level (weather_com.py):**
- Always attempts fresh API call (no premature cache returns)
- Rate limit (6/day) only uses cache if cache is < 6 hours old
- If rate limit reached AND cache is stale, rate limit is overridden — freshness always wins
- Fallback chain: TWC API → web scraping → fresh cache (< 6h) → None
- `precip_prob` extracted from `daypart[0].precipChance` daytime value (matches weather.com website display; was previously hardcoded to 0, then briefly used max(day,night) which inflated values)
- `condition` extracted from `daypart[0].wxPhraseLong` (was previously truncated narrative)

**Provider-level (wunderground.py):**
- Fetch chain: page scrape → TWC API (apiKey harvested from that page) → TWC API
  (env key) → fresh cache (< 6h **and** still covering today) → None. The page
  leads because it is the alignment target; the env-key call is last because its
  geocode is configured rather than harvested and can drift from the 95350 page
- The harvest step is the one that survives wunderground.com going fully
  client-rendered: the page returns 200 with no forecast arrays in it, so the
  provider pulls the `apiKey`/`geocode` out of the page's own JS and calls the
  same v3 endpoint the site's front end calls. Key precedence:
  `WUNDERGROUND_API_KEY` → `TWC_API_KEY` → harvested. Geocode precedence:
  `WUNDERGROUND_GEOCODE` → harvested → the `37.66,-121.00` default
- Embedded JSON is parsed in **both** forms the site has shipped: raw
  (`"temperatureMax":[…]`) and JS-escaped (`JSON.parse("{\"temperatureMax\":…")`),
  minified or pretty-printed. A regex written for only one form reads exactly
  like a dead provider
- The page carries several forecast contexts and **hourly arrays reuse the daily
  field names**, so a first-match regex can land on the wrong one. Daypart
  arrays are chosen by length (2 entries per day); dates on the scrape path stay
  **index-based** — a `validTimeLocal` pulled blind out of the page can be 24
  hourly stamps sharing one date, which collapses the whole row into one column.
  Only the API path, whose response shape is unambiguous, dates rows from
  `validTimeLocal`
- The page blob yields only **6 days** against the 8-column grid, which dashes
  the last two columns on every run. When the scrape covers fewer than
  `GRID_DAYS`, the provider extends it through the v3 API using the key/geocode
  harvested from that same page. Strictly additive: a failed call keeps the
  scraped days
- Impersonation fingerprints are tried in order (`firefox135`, `chrome136`,
  `chrome120`, `chrome110`) rather than hardcoding one: curl_cffi **raises** on a
  target its build doesn't know, and `curl-cffi>=0.7.0` is unpinned
- Cached days dated before today are dropped, and a cache that no longer covers
  today is rejected outright — serving it produces an all-dash row, not an error
- Same daytime-daypart rule as weather_com for `precip_prob` / `condition`
- `temperatureMax[0]` goes null after today's high passes; `calendarDayTemperatureMax`
  fills that cell instead of dropping today's column

**CacheManager-level (cache_manager.py):**
- Per-provider `MAX_CACHE_HOURS` thresholds enforced:
  - 18h: weather_com, wunderground, accuweather, google_weather
  - 24h: noaa, open_meteo
  - 12h: hrrr
  - 48h: mid_org
- Cache exceeding max age is **rejected** — falls through to DEFAULT values
- Stale data is never silently served as if it were valid

**SSL-level (ssl_helper.py):**
- httpx: `pip-system-certs` patches Python's `ssl.create_default_context()` to use the OS cert store
- curl_cffi: `ssl_helper.py` exports the Windows cert store to a PEM file so libcurl trusts the same CAs as Windows
- This handles firewall SSL inspection: if the firewall's CA is in the Windows cert store, curl_cffi will trust it
- Priority for curl_cffi: `DUCK_SUN_CA_BUNDLE` env var → Windows cert store export → certifi bundle → curl default
- **No `verify=False` anywhere in the codebase** — all connections use proper SSL verification
- For PyInstaller exe: bundle certifi with `--collect-data certifi`

### How To Diagnose Stale Data
If weather.com temps in the report don't match the website:
1. Check for SSL errors in the terminal output (certificate verification failures)
2. Compare report temps against `outputs/weathercom_cache.json` timestamp
3. Compare against `outputs/cache/weather_com_lkg.json` timestamp
4. If both are old, the API call is failing — check `TWC_API_KEY` and verify `pip-system-certs` is installed
5. Both weather_com.py and wunderground.py use curl_cffi — if one fails, the other likely does too
6. If firewall is blocking connections, contact IT Systems (Scott Bays) for domain whitelisting

### How To Diagnose a Blank (All-Dash) Source Row

First separate the two shapes, because they have different causes:

- **Trailing dashes** (row populated, last N columns `--`) — the provider
  returned fewer days than the grid is wide. WUnderground's page scrape returns
  6 against an 8-column grid, which is why its last two columns were empty on
  every run before the API top-up landed. `<SOURCE>: 6/8 grid days` in the log
- **All dashes** — the provider returned nothing, or returned a forecast whose
  **dates don't overlap the grid**. Both look identical in the spreadsheet

The run log is the source of truth:

1. `logs/duck_sun.log` — `[generate_excel_report] <SOURCE>: 0/8 grid days - row
   will be ALL DASHES` is written before the sheet is drawn, and
   `[main] Data validation` lists a day count per provider (0 = blank row).
   Weather.com and WUnderground are warn-only there: a blocked scraper reports
   loudly but never stalls the report
2. `outputs/cache/<provider>_lkg.json` — its `timestamp` is the last time that
   provider actually succeeded. A months-old timestamp means the live fetch has
   been failing since then and nobody noticed
3. The row goes blank rather than stale because `CacheManager` rejects cache
   past `MAX_CACHE_HOURS` and `DEFAULT_VALUES` has **no** `wunderground` entry
   (`{}` → falsy → blank row). That is deliberate: blank is honest, a fabricated
   55/40 is not. Note `weather_com` **does** still carry placeholder defaults,
   which would enter the weighted average if it ever fell that far
4. Run the provider standalone to see which link in the chain broke:
   `./venv/Scripts/python.exe -m duck_sun.providers.wunderground`

## Google-First Policy (Jul 2026)

Google Weather (MetNet-3) is the preferred source **everywhere on the report
except the word-descriptor row**. Three rules follow from that:

**1. Google is never demoted.** `ensemble.py` used to run a "Google Veto"
that dropped Google's weight at >6°F deviation from the peer median, and again
at >10°F. That is gone. Google holds weight 8.0 in every hour,
unconditionally. The deviation is still measured and reported in
`diagnostics["google_peer_delta_f"]` for observability, but nothing acts on it.
Rationale: Google has consistently been the most accurate source, so a
disagreement with its peers is evidence against the peers.
`google_veto_triggered` / `google_veto_severity` remain in the diagnostics dict
(always `False` / `None`) so downstream consumers don't break.

**2. Solar is Google-first, not a hybrid.** `calculate_hybrid_solar()` keeps its
name but is no longer a blend. Priority is now:
   1. Google MetNet-3 cloud cover against the clear-sky ceiling
   2. Open-Meteo shortwave radiation — **only** when Google has no value for
      that hour (`google_cloud is None`)
   3. Zero

   Pass `None`, never a placeholder like `50`, when Google has no reading — a
   default is indistinguishable from a real 50% sky and silently fabricates a
   half-clouded hour. `uncanniness.py` uses `google_cloud_map.get(row_time)`
   with no default for exactly this reason.

   The Excel grid and the physics engine now share one irradiance model
   (`solar_physics.calculate_solar_from_cloud_cover`), so the grid and the JSON
   outlook can't drift apart. Watch for these log lines:
   - `Solar grid source mix: 64/64 cells from Google MetNet-3 (100%)`
   - `Solar source mix: N hours Google (MetNet-3), 0 hours Open-Meteo fallback`

   Any Open-Meteo fallback logs at WARNING level — it means Google was degraded.

**3. Word descriptors come from Weather.com.** This is the deliberate exception.
Priority is `Weather.com > AccuWeather > Google > Open-Meteo`. Weather.com's
`daypart[0].wxPhraseLong` is verbose, so `fit_condition_text()` abbreviates
known long phrases and otherwise drops whole trailing words — never cuts
mid-word (the old hard truncation produced `"Sunshine And C"`). Add new
phrasings to `CONDITION_ABBREVIATIONS` in `excel_report.py` as they show up.

**PRECIP % is still Weather.com-primary**, deliberately, per the Feb 2026
stale-data incident documented below — that row must match the weather.com
website 1:1. Google remains the first fallback.

## Google Weather API Limits (verified Jul 2026)

There is **no tier system that affects forecast length**. The Weather API is a
single per-call SKU:

| | |
|---|---|
| Free allowance | 10,000 calls/month per SKU |
| Overage | $0.15 per 1,000 calls |
| Default rate limit | 6,000 queries/minute per project (adjustable in Cloud Console) |
| Forecast horizon | `hours` = 1..240 on `forecast/hours:lookup`, independent of spend |

Current usage: 10 paginated calls for Modesto + 10 for Portland = **20 per run**.
At one run/day that's ~600/month against a 10,000 free allowance — about 6%, at
no cost. Room for ~16 runs/day before billing starts. A pay-as-you-go project
needs billing *enabled* (a card on file) even while inside the free allowance;
that is not a "tier", just Google's activation requirement.

## Portland, OR Side Reference (Jul 2026)

The Excel one-pager carries a single Portland, OR Hi/Lo line directly beneath the
Modesto block and above the solar grid. It is a **reference only**:

- Sourced from the same Google Weather (MetNet-3) API, 240-hour pull, at
  45.5152 / -122.6784 via `GooglePortlandProvider`
- Portland shares Modesto's Pacific timezone, so its calendar-day highs/lows
  line up column-for-column with the Modesto grid - no date shifting
- **Never** enters the weighted average. The consensus formula still spans only
  source rows 13-18; Portland lives on row 24
- Carries its own repeated day-name row (row 23). By that point the reader is a
  dozen rows below the Modesto header, so the labels are repeated rather than
  making them scroll back up to map columns to days
- Uses its own cache key (`google_portland`), so a Portland fetch can never
  overwrite the Modesto Last Known Good data
- Non-critical: a missing Portland forecast logs a warning and blanks the row
  with `--`. It never triggers a report retry or blocks the Modesto forecast

### Excel row map (`duck_sun/excel_report.py`)

| Rows | Content |
|------|---------|
| 1-8 | Title, timestamp, PGE CITYGATE / MID GAS NOM, MID 48-hour summary |
| 10-12 | Condition descriptors, day names, dates |
| 13-18 | The 6 Modesto sources (weighted-average formula range) |
| 19-21 | Wtd. Average, PRECIP %, precip source note |
| **22-24** | **Portland, OR banner + day names + Hi/Lo reference row** |
| 26-41 | Solar forecast title, header, 7 days x 2 rows |
| 43 | Solar legend |

## Removed Sources

**Met.no (removed Jul 2026).** The Norwegian Met Institute / ECMWF feed was
dropped as a data source: it was consistently among the worst performers, and
it was the source most often flagged as an outlier in the hourly variance logs.
The ensemble is now **6 sources**, not 7:

| Excel row | Source | Weight |
|-----------|--------|--------|
| 13 | OPEN-METEO | 1 |
| 14 | NOAA (GOV) | 3 |
| 15 | ACCUWEATHER | 4 |
| 16 | WEATHER.COM | 4 |
| 17 | WUNDERGRND | 4 |
| 18 | GOOGLE (AI) | 8 |

The weighted-average formula spans `13:18` with the array `{1;3;4;4;4;8}`.
`duck_sun/providers/met_no.py` is deleted - recover it from git history if it
ever needs to come back, and remember to re-add the row, re-widen the formula
range, and shift every row below it back down by one.

**Smoke / PM2.5 (removed Feb 2026, commit 34460a7).** `providers/smoke.py` was
deleted because IT Security flagged `air-quality-api.open-meteo.com`. The
engine's Smoke Guard logic is still present in `uncanniness.py` but receives no
PM2.5 input, so every hour scores `smoke_factor` 1.0. Do **not** reintroduce
the provider without a fresh IT review.

Row numbers 19 and 13-18 are asserted by `tests/test_excel_report_formulas.py`;
the Portland band and the shifted solar block are asserted by
`tests/test_portland_reference.py`.
