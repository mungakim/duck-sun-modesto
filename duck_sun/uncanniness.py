"""
Uncanny Engine for Duck Sun Modesto

Architecture:
1. Thermodynamics: WEIGHTED ENSEMBLE (NOAA 5x > AccuWeather 3x > Met.no 3x > Weather.com 2x > Open-Meteo 1x)
2. Energy: Open-Meteo (Physics)
3. Logic Override: NOAA Text Narratives ("Dense Fog") force the model's hand.
4. Variance Detection: Flags high spread (>10°F) with WARN-ONLY alerts (never blocks)

RELIABILITY IS KING - Consistent, accurate values every time.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

from duck_sun.ensemble import WeightedEnsembleEngine, ConsensusResult
from duck_sun.solar_physics import (
    calculate_hybrid_solar,
    calculate_tule_fog_penalty,
    get_irradiance_category
)

logger = logging.getLogger(__name__)


class UncannyEngine:
    """
    The Hybrid Architecture Engine with WEIGHTED ENSEMBLE Consensus.

    Temperature consensus: Weighted Ensemble (NOAA 5x > AccuWeather 3x > Met.no 3x > Weather.com 2x > Open-Meteo 1x)
    Solar physics: Always Open-Meteo radiation data
    Logic override: NOAA text narratives trigger fog probability boosts
    Variance detection: Flags high spread (>10°F) with WARN-ONLY alerts

    RELIABILITY IS KING - This engine prioritizes consistent, accurate values.
    """

    DEW_POINT_DEPRESSION_THRESHOLD = 2.5
    WIND_STAGNATION_THRESHOLD = 8.0
    FOG_HOURS_START = 8   # HE08
    FOG_HOURS_END = 13    # HE13
    FOG_SOLAR_PENALTY = 0.15

    SMOKE_TIERS = [(25, 1.00), (50, 0.95), (100, 0.85), (200, 0.70), (999, 0.50)]

    def __init__(self):
        logger.info("[UncannyEngine] Initializing Weighted Ensemble Architecture engine...")
        self.timezone = ZoneInfo("America/Los_Angeles")
        self.ensemble_engine = WeightedEnsembleEngine()
        self.variance_results: List[ConsensusResult] = []  # Track variance for reporting

    def normalize_temps(
        self,
        om_data: Dict[str, Any],
        noaa_data: Optional[List[Dict]],
        met_no_data: Optional[List[Dict]],
        accu_data: Optional[List[Dict]] = None,
        weathercom_data: Optional[List[Dict]] = None,
        mid_data: Optional[Dict] = None,
        smoke_data: Optional[List[Dict]] = None
    ) -> pd.DataFrame:
        """
        Merge temps using WEIGHTED ENSEMBLE strategy.

        Sources (weighted):
        - NOAA: 5.0 (highest trust)
        - AccuWeather: 3.0
        - Met.no: 3.0
        - Weather.com: 2.0 (baseline reference)
        - MID.org: 2.0 (local microclimate)
        - Open-Meteo: 1.0 (fallback)

        Variance Detection:
        - LOW: spread < 5°F
        - MODERATE: spread 5-10°F (yellow warning)
        - CRITICAL: spread > 10°F (red warning)

        NEVER BLOCKS - Warnings are informational only.
        """
        logger.info("[UncannyEngine] Building WEIGHTED ENSEMBLE consensus model...")
        self.variance_results = []  # Reset variance tracking

        hourly = om_data.get('daily_summary', [])
        if not hourly:
            logger.error("[UncannyEngine] No hourly data in Open-Meteo result")
            raise ValueError("No hourly data")

        df = pd.DataFrame(hourly)
        df['time'] = pd.to_datetime(df['time'])
        df = df.rename(columns={'temperature_c': 'temp_om'})

        logger.info(f"[UncannyEngine] Base data: {len(df)} hours from Open-Meteo")

        # === MERGE ALL SOURCE TEMPERATURES ===

        # NOAA temperatures
        df['temp_noaa'] = np.nan
        if noaa_data:
            noaa_df = pd.DataFrame(noaa_data)
            noaa_df['time'] = pd.to_datetime(noaa_df['time'], utc=True).dt.tz_convert(self.timezone).dt.tz_localize(None)

            noaa_merged = 0
            for idx, row in df.iterrows():
                matches = noaa_df[(noaa_df['time'] >= row['time'] - timedelta(minutes=30)) &
                                 (noaa_df['time'] <= row['time'] + timedelta(minutes=30))]
                if not matches.empty:
                    df.at[idx, 'temp_noaa'] = matches.iloc[0]['temp_c']
                    noaa_merged += 1

            logger.info(f"[UncannyEngine] Merged {noaa_merged} NOAA temperature records")
        else:
            logger.warning("[UncannyEngine] No NOAA data available")

        # Met.no temperatures
        df['temp_met'] = np.nan
        if met_no_data:
            met_df = pd.DataFrame(met_no_data)
            met_df['time'] = pd.to_datetime(met_df['time'], utc=True).dt.tz_convert(self.timezone).dt.tz_localize(None)

            met_merged = 0
            for idx, row in df.iterrows():
                matches = met_df[(met_df['time'] >= row['time'] - timedelta(minutes=30)) &
                                 (met_df['time'] <= row['time'] + timedelta(minutes=30))]
                if not matches.empty:
                    df.at[idx, 'temp_met'] = matches.iloc[0]['temp_c']
                    met_merged += 1

            logger.info(f"[UncannyEngine] Merged {met_merged} Met.no temperature records")
        else:
            logger.warning("[UncannyEngine] No Met.no data available")

        # AccuWeather daily temps (interpolate to hourly by day)
        df['temp_accu'] = np.nan
        if accu_data:
            # Filter to only days with valid temperature data
            accu_by_date = {d['date']: d for d in accu_data
                           if d.get('low_c') is not None and d.get('high_c') is not None}
            accu_merged = 0
            for idx, row in df.iterrows():
                date_str = row['time'].strftime('%Y-%m-%d')
                if date_str in accu_by_date:
                    # Use daily average as hourly proxy (rough approximation)
                    day_data = accu_by_date[date_str]
                    hour = row['time'].hour
                    # Simple diurnal interpolation: morning=low, afternoon=high
                    if 6 <= hour < 10:
                        temp = day_data['low_c'] + (day_data['high_c'] - day_data['low_c']) * 0.3
                    elif 10 <= hour < 16:
                        temp = day_data['high_c'] - (day_data['high_c'] - day_data['low_c']) * 0.1
                    elif 16 <= hour < 20:
                        temp = day_data['high_c'] - (day_data['high_c'] - day_data['low_c']) * 0.4
                    else:
                        temp = day_data['low_c'] + (day_data['high_c'] - day_data['low_c']) * 0.1
                    df.at[idx, 'temp_accu'] = temp
                    accu_merged += 1
            logger.info(f"[UncannyEngine] Interpolated {accu_merged} AccuWeather records")
        else:
            logger.info("[UncannyEngine] No AccuWeather data available")

        # Weather.com temperatures
        df['temp_weathercom'] = np.nan
        if weathercom_data:
            wc_by_date = {d['date']: d for d in weathercom_data}
            wc_merged = 0
            for idx, row in df.iterrows():
                date_str = row['time'].strftime('%Y-%m-%d')
                if date_str in wc_by_date:
                    day_data = wc_by_date[date_str]
                    high_c = day_data.get('high_c')
                    low_c = day_data.get('low_c')
                    # Skip if high_c is None (e.g., tonight forecast)
                    if high_c is None or low_c is None:
                        continue
                    hour = row['time'].hour
                    # Same diurnal interpolation
                    if 6 <= hour < 10:
                        temp = low_c + (high_c - low_c) * 0.3
                    elif 10 <= hour < 16:
                        temp = high_c - (high_c - low_c) * 0.1
                    elif 16 <= hour < 20:
                        temp = high_c - (high_c - low_c) * 0.4
                    else:
                        temp = low_c + (high_c - low_c) * 0.1
                    df.at[idx, 'temp_weathercom'] = temp
                    wc_merged += 1
            logger.info(f"[UncannyEngine] Interpolated {wc_merged} Weather.com records")
        else:
            logger.info("[UncannyEngine] No Weather.com data available")

        # MID.org temperature (single observation - apply to current day only)
        df['temp_mid'] = np.nan
        if mid_data and mid_data.get('high_c') is not None:
            mid_date = mid_data.get('date', datetime.now().strftime('%Y-%m-%d'))
            mid_merged = 0
            for idx, row in df.iterrows():
                if row['time'].strftime('%Y-%m-%d') == mid_date:
                    hour = row['time'].hour
                    high_c = mid_data.get('high_c', 20)
                    low_c = mid_data.get('low_c', 10)
                    if 6 <= hour < 10:
                        temp = low_c + (high_c - low_c) * 0.3
                    elif 10 <= hour < 16:
                        temp = high_c - (high_c - low_c) * 0.1
                    elif 16 <= hour < 20:
                        temp = high_c - (high_c - low_c) * 0.4
                    else:
                        temp = low_c + (high_c - low_c) * 0.1
                    df.at[idx, 'temp_mid'] = temp
                    mid_merged += 1
            logger.info(f"[UncannyEngine] Applied {mid_merged} MID.org records")
        else:
            logger.info("[UncannyEngine] No MID.org data available")

        # === COMPUTE WEIGHTED ENSEMBLE CONSENSUS ===
        df['temp_consensus'] = np.nan
        df['variance_level'] = "LOW"
        df['variance_spread_f'] = 0.0
        df['outlier_sources'] = ""

        variance_counts = {"LOW": 0, "MODERATE": 0, "CRITICAL": 0}

        for idx, row in df.iterrows():
            # Build source dict for this hour
            sources = {
                "NOAA": row['temp_noaa'] if pd.notna(row['temp_noaa']) else None,
                "AccuWeather": row['temp_accu'] if pd.notna(row.get('temp_accu')) else None,
                "Met.no": row['temp_met'] if pd.notna(row['temp_met']) else None,
                "Weather.com": row['temp_weathercom'] if pd.notna(row.get('temp_weathercom')) else None,
                "MID.org": row['temp_mid'] if pd.notna(row.get('temp_mid')) else None,
                "Open-Meteo": row['temp_om'] if pd.notna(row['temp_om']) else None,
            }

            # Compute weighted consensus
            result = self.ensemble_engine.compute_consensus(sources, unit="C")
            self.variance_results.append(result)

            df.at[idx, 'temp_consensus'] = result.consensus_value
            df.at[idx, 'variance_level'] = result.variance_level
            df.at[idx, 'variance_spread_f'] = result.spread_f

            # Track outliers
            if result.outliers:
                outlier_names = [o[0] for o in result.outliers]
                df.at[idx, 'outlier_sources'] = ", ".join(outlier_names)

            variance_counts[result.variance_level] = variance_counts.get(result.variance_level, 0) + 1

        # Log variance summary
        logger.info(f"[UncannyEngine] Variance summary: LOW={variance_counts['LOW']}, "
                   f"MODERATE={variance_counts['MODERATE']}, CRITICAL={variance_counts['CRITICAL']}")

        if variance_counts['CRITICAL'] > 0:
            logger.warning(f"[UncannyEngine] VARIANCE WARNING: {variance_counts['CRITICAL']} hours "
                          f"with >10°F spread detected")

        # === MERGE SMOKE DATA ===
        df['pm2_5'] = 0.0
        df['us_aqi'] = 0

        if smoke_data:
            smoke_df = pd.DataFrame(smoke_data)
            smoke_df['time'] = pd.to_datetime(smoke_df['time'])

            smoke_merged = 0
            for idx, row in df.iterrows():
                matches = smoke_df[smoke_df['time'] == row['time']]
                if not matches.empty:
                    df.at[idx, 'pm2_5'] = matches.iloc[0]['pm2_5']
                    df.at[idx, 'us_aqi'] = matches.iloc[0].get('us_aqi', 0)
                    smoke_merged += 1

            max_pm = df['pm2_5'].max()
            logger.info(f"[UncannyEngine] Merged {smoke_merged} smoke records (Max PM2.5: {max_pm:.1f})")

            if max_pm > 100:
                logger.warning(f"[UncannyEngine] SMOKE ALERT: PM2.5 > 100 ug/m3 detected!")
        else:
            logger.info("[UncannyEngine] No smoke data available - assuming clear air")

        return df

    def get_variance_report(self) -> Dict[str, Any]:
        """Get variance report from last normalize_temps() call."""
        if not self.variance_results:
            return {"total": 0, "message": "No variance data - run normalize_temps() first"}
        return self.ensemble_engine.get_variance_report(self.variance_results)

    def analyze_duck_curve(
        self,
        df: pd.DataFrame,
        google_hourly: Optional[List[Dict]] = None,
        noaa_text_data: Optional[List[Dict]] = None
    ) -> pd.DataFrame:
        """
        Apply Hybrid Solar Physics + Fog/Smoke Detection.

        The new architecture:
        1. Uses calculate_hybrid_solar() which fuses Open-Meteo physics with Google cloud timing
        2. Applies Tule Fog specific penalties for Central Valley radiation fog
        3. Applies smoke/AQI penalties
        4. Uses NOAA narrative override for fog warnings
        """
        logger.info("[UncannyEngine] Running Hybrid Solar Physics + Fog Guard + Smoke Guard...")

        df['solar_adjusted'] = df['radiation'].copy()
        df['risk_level'] = "LOW"
        df['fog_probability'] = 0.0
        df['smoke_penalty'] = 1.0
        df['hybrid_source'] = "Open-Meteo"  # Track which source is used

        # Build Google Cloud Cover map from hourly data
        google_cloud_map = {}
        if google_hourly:
            logger.info(f"[UncannyEngine] Processing {len(google_hourly)} Google hourly records for cloud timing...")
            for h in google_hourly:
                try:
                    time_str = h.get('time', '')
                    if not time_str:
                        continue
                    # Parse time and convert to local
                    if 'Z' in time_str:
                        dt = datetime.fromisoformat(time_str.replace('Z', '+00:00')).astimezone(self.timezone)
                    else:
                        dt = datetime.fromisoformat(time_str).astimezone(self.timezone)
                    # Remove timezone for matching with df
                    dt_naive = dt.replace(tzinfo=None)
                    google_cloud_map[dt_naive] = h.get('cloud_cover', 50)
                except Exception as e:
                    logger.debug(f"[UncannyEngine] Error parsing Google time: {e}")
                    continue
            logger.info(f"[UncannyEngine] Mapped {len(google_cloud_map)} Google cloud observations")

        # Check text forecast for fog keywords (Narrative Override)
        text_mentions_fog = False
        if noaa_text_data:
            logger.info("[UncannyEngine] Scanning NOAA text forecast for fog keywords...")
            for p in noaa_text_data[:4]:  # Check next ~48 hours
                text = p.get('detailedForecast', '').lower()
                if 'dense fog' in text or 'patchy fog' in text or 'areas of fog' in text:
                    text_mentions_fog = True
                    logger.warning(f"[UncannyEngine] NARRATIVE OVERRIDE: '{p['name']}' mentions fog!")
                    break

        is_fog_locked_in = False
        fog_hours_detected = 0
        tule_fog_hours = 0
        lock_in_hours = 0
        smoke_hours_detected = 0
        hybrid_solar_used = 0

        for idx, row in df.iterrows():
            hour = row['time'].hour
            day_of_year = row['time'].timetuple().tm_yday

            # Reset lock at midnight (new day logic)
            if hour == 0:
                is_fog_locked_in = False

            # === 1. HYBRID SOLAR CALCULATION ===
            # Get Google cloud cover for this hour (default to 50% if not available)
            row_time = row['time']
            if hasattr(row_time, 'to_pydatetime'):
                row_time = row_time.to_pydatetime()
            if row_time.tzinfo is not None:
                row_time = row_time.replace(tzinfo=None)

            google_cloud = google_cloud_map.get(row_time, 50)
            om_radiation = row.get('radiation', 0)

            # Calculate hybrid solar using the new physics module
            if om_radiation > 0 or google_cloud < 100:
                hybrid_watts = calculate_hybrid_solar(
                    om_radiation=om_radiation,
                    google_cloud=google_cloud,
                    hour=hour,
                    day_of_year=day_of_year
                )
                df.at[idx, 'solar_adjusted'] = hybrid_watts
                df.at[idx, 'hybrid_source'] = "Hybrid (OM+Google)"
                hybrid_solar_used += 1
            else:
                # No data available - use radiation as-is
                df.at[idx, 'solar_adjusted'] = om_radiation

            # === 2. SMOKE GUARD (Applies 24/7) ===
            pm = row.get('pm2_5', 0)
            smoke_factor = 1.0
            for limit, factor in self.SMOKE_TIERS:
                if pm <= limit:
                    smoke_factor = factor
                    break
            df.at[idx, 'smoke_penalty'] = smoke_factor

            if smoke_factor < 1.0:
                df.at[idx, 'solar_adjusted'] = df.at[idx, 'solar_adjusted'] * smoke_factor
                if pm > 100:
                    smoke_hours_detected += 1
                    df.at[idx, 'risk_level'] = f"SMOKE ({int(pm)} ug/m3)"

            # === 3. TULE FOG SPECIFIC CHECK (Physics + Conditions) ===
            temp = row['temp_consensus']
            dewpoint = row.get('dewpoint_c', row.get('dewpoint', 0))
            wind = row.get('wind_speed_kmh', row.get('wind', 0))

            # Use new Tule Fog penalty calculation
            tule_penalty = calculate_tule_fog_penalty(temp, dewpoint, wind, hour)

            if tule_penalty < 0.2:
                # Severe Tule Fog - apply massive penalty
                current_solar = df.at[idx, 'solar_adjusted']
                df.at[idx, 'solar_adjusted'] = current_solar * tule_penalty
                df.at[idx, 'risk_level'] = "CRITICAL (TULE FOG)"
                tule_fog_hours += 1
                logger.warning(f"[UncannyEngine] TULE FOG at {row['time']}: penalty={tule_penalty:.2f}")
            elif tule_penalty < 0.5:
                # Moderate Tule Fog risk
                current_solar = df.at[idx, 'solar_adjusted']
                df.at[idx, 'solar_adjusted'] = current_solar * tule_penalty
                df.at[idx, 'risk_level'] = "HIGH (TULE FOG RISK)"

            # === 4. STANDARD FOG GUARD (Fallback) ===
            dp_depression = temp - dewpoint

            # Calculate fog probability
            depression_factor = max(0, 1 - (dp_depression / self.DEW_POINT_DEPRESSION_THRESHOLD))
            stagnation_factor = max(0, 1 - (wind / self.WIND_STAGNATION_THRESHOLD))
            fog_prob = round(depression_factor * stagnation_factor, 2)

            # NARRATIVE OVERRIDE: If NOAA text mentions fog, boost probability
            if text_mentions_fog and fog_prob > 0.3:
                fog_prob = min(0.99, fog_prob + 0.3)
                logger.debug(f"[UncannyEngine] Fog prob boosted by narrative: {fog_prob:.2f}")

            df.at[idx, 'fog_probability'] = fog_prob

            # Pre-Dawn Lock-In Check
            if 4 <= hour < 8 and fog_prob > 0.8:
                is_fog_locked_in = True
                lock_in_hours += 1
                logger.warning(f"[UncannyEngine] PRE-DAWN LOCK at {row['time']}: fog_prob={fog_prob:.2f}")

            # Apply Standard Fog Penalties during sun hours (if not already penalized by Tule Fog)
            if self.FOG_HOURS_START <= hour <= self.FOG_HOURS_END and "TULE FOG" not in df.at[idx, 'risk_level']:
                current_adj = df.at[idx, 'solar_adjusted']

                if fog_prob > 0.85:
                    fog_solar = row['radiation'] * self.FOG_SOLAR_PENALTY
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "CRITICAL (ACTIVE FOG)"
                        fog_hours_detected += 1

                elif is_fog_locked_in:
                    fog_solar = row['radiation'] * 0.40
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "HIGH (PERSISTENT STRATUS)"

                elif fog_prob > 0.5:
                    fog_solar = row['radiation'] * 0.7
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "MODERATE (RISK)"

        # Final summary logging
        logger.info(f"[UncannyEngine] Hybrid solar calculations: {hybrid_solar_used} hours processed")
        if tule_fog_hours > 0:
            logger.warning(f"[UncannyEngine] TULE FOG ALERT: {tule_fog_hours} hours with Central Valley radiation fog")
        if lock_in_hours > 0:
            logger.warning(f"[UncannyEngine] FOG LOCK-IN: {lock_in_hours} pre-dawn hours triggered inversion")
        if fog_hours_detected > 0:
            logger.warning(f"[UncannyEngine] ACTIVE FOG: {fog_hours_detected} daytime hours CRITICAL")
        if smoke_hours_detected > 0:
            logger.warning(f"[UncannyEngine] SMOKE IMPACT: {smoke_hours_detected} hours with PM2.5 > 100")

        if not is_fog_locked_in and fog_hours_detected == 0 and smoke_hours_detected == 0 and tule_fog_hours == 0:
            logger.info("[UncannyEngine] No Tule Fog or significant smoke detected - Clear forecast")

        return df

    def get_daily_summary(self, df: pd.DataFrame, days: int = 8) -> List[Dict]:
        """Generate daily summary from analyzed data."""
        logger.info(f"[UncannyEngine] Generating {days}-day summary...")
        
        df_copy = df.copy()
        df_copy['date'] = df_copy['time'].dt.date
        
        daily = df_copy.groupby('date').agg({
            'temp_consensus': 'mean',
            'radiation': 'mean',
            'solar_adjusted': 'mean',
            'cloud_cover': 'mean',
            'pm2_5': 'max'
        }).reset_index().head(days)
        
        summaries = []
        for _, row in daily.iterrows():
            summaries.append({
                "date": str(row['date']),
                "temp_consensus_c": round(row['temp_consensus'], 1),
                "avg_solar_raw": round(row['radiation'], 0),
                "avg_solar_adjusted": round(row['solar_adjusted'], 0),
                "avg_cloud_cover": round(row['cloud_cover'], 0),
                "max_pm2_5": round(row['pm2_5'], 1)
            })
        
        logger.info(f"[UncannyEngine] Generated {len(summaries)} daily summaries")
        return summaries

    def get_duck_curve_hours(self, df: pd.DataFrame) -> List[Dict]:
        """Extract HE09-HE16 hours for tomorrow."""
        target = (datetime.now(self.timezone) + timedelta(days=1)).date()
        mask = (df['time'].dt.date == target) & (df['time'].dt.hour.between(9, 16))
        duck_df = df[mask]
        
        logger.info(f"[UncannyEngine] Extracting duck curve hours for {target}")
        
        hours = []
        for _, row in duck_df.iterrows():
            hours.append({
                "time": row['time'].strftime("%Y-%m-%dT%H:%M"),
                "temp_consensus": row['temp_consensus'],
                "solar_adjusted": row['solar_adjusted'],
                "risk_level": row['risk_level'],
                "fog_probability": row['fog_probability'],
                "pm2_5": row.get('pm2_5', 0)
            })
        
        logger.info(f"[UncannyEngine] Extracted {len(hours)} duck curve hours")
        return hours


if __name__ == "__main__":
    import asyncio
    from duck_sun.providers.open_meteo import fetch_open_meteo
    from duck_sun.providers.noaa import NOAAProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.providers.accuweather import AccuWeatherProvider
    from duck_sun.providers.mid_org import MIDOrgProvider

    logging.basicConfig(level=logging.INFO)

    async def test():
        print("=== Testing Uncanny Engine (WEIGHTED ENSEMBLE Architecture) ===\n")

        print("Fetching Open-Meteo...")
        om_data = await fetch_open_meteo(days=3)

        print("Fetching NOAA...")
        noaa = NOAAProvider()
        noaa_data = await noaa.fetch_async()
        noaa_text = await noaa.fetch_text_forecast()

        print("Fetching Met.no...")
        met = MetNoProvider()
        met_data = await met.fetch_async()

        print("Fetching AccuWeather...")
        accu = AccuWeatherProvider()
        accu_data = await accu.fetch_forecast()

        print("Fetching MID.org (local)...")
        mid = MIDOrgProvider()
        mid_data = await mid.fetch_48hr_summary()

        engine = UncannyEngine()

        print("\nBuilding WEIGHTED ENSEMBLE consensus model...")
        df = engine.normalize_temps(
            om_data, noaa_data, met_data,
            accu_data=accu_data,
            mid_data=mid_data
        )

        print("\n=== Variance Report ===")
        var_report = engine.get_variance_report()
        print(f"  Total calculations: {var_report.get('total', 0)}")
        print(f"  Variance counts: {var_report.get('variance_counts', {})}")
        print(f"  Avg confidence: {var_report.get('avg_confidence', 0):.2f}")
        if var_report.get('has_critical'):
            print(f"  WARNING: Critical variance detected!")

        print("\nRunning Fog Guard + Narrative Override...")
        df_analyzed = engine.analyze_duck_curve(df, noaa_text_data=noaa_text)

        print("\n=== Daily Summary ===")
        for day in engine.get_daily_summary(df_analyzed, days=3):
            print(f"  {day['date']}: {day['temp_consensus_c']}C "
                  f"(solar: {day['avg_solar_adjusted']:.0f} W/m²)")

        print("\n=== Tomorrow's Duck Curve ===")
        duck_hours = engine.get_duck_curve_hours(df_analyzed)
        for hour in duck_hours:
            print(f"  {hour['time'][-5:]}: {hour['solar_adjusted']:.0f} W/m² "
                  f"[{hour['risk_level']}]")

    asyncio.run(test())
