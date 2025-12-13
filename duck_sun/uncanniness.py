"""
Uncanny Engine for Duck Sun Modesto

This module implements the "Fog Guard" and "Smoke Guard" logic along with
the consensus temperature model. It triangulates data from multiple weather
sources and detects both Tule Fog and wildfire smoke conditions that can
impact solar production in the Central Valley.

Key Physics:
- Tule Fog forms when dewpoint depression < 2.5C AND wind < 8 km/h
- During winter mornings (HE08-HE13), fog can reduce solar by 85%
- The "Modesto Bowl" topography traps cold air and fog
- Wildfire smoke scatters solar differently, "flattening" the curve
- PM2.5 > 100 ug/m3 can reduce solar by 15-30%

The "Smoke Shade" Effect:
Smoke creates a persistent haze that reduces Direct Normal Irradiance (DNI)
while maintaining some diffuse irradiance. Unlike clouds which create
intermittent shadows, smoke creates a steady reduction in solar output.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, TypedDict
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class AnalyzedHour(TypedDict):
    """Typed dictionary for analyzed hourly data."""
    time: str
    temp_consensus: float
    temp_om: float  # Open-Meteo
    temp_nws: Optional[float]  # NWS (may be None)
    temp_met: Optional[float]  # Met.no (may be None)
    dewpoint: float
    wind_kmh: float
    solar_raw: float
    solar_adjusted: float
    cloud_cover: int
    risk_level: str
    fog_probability: float
    pm2_5: float           # NEW: PM2.5 concentration (ug/m3)
    smoke_penalty: float   # NEW: Smoke multiplier (0.0-1.0)


class UncannyEngine:
    """
    The "Fog Guard" & "Smoke Guard" engine.

    Combines multiple temperature sources into a consensus and applies
    physics-based fog and smoke detection to adjust solar production estimates.
    
    The engine implements three-layer protection:
    1. Global Consensus: Temperature triangulation (Open-Meteo/NWS/Met.no)
    2. Local Physics: Fog detection (dewpoint depression + wind stagnation)
    3. Environmental Hazard: Smoke detection (PM2.5 concentration)
    """

    # Fog detection thresholds (Central Valley calibrated)
    DEW_POINT_DEPRESSION_THRESHOLD = 2.5  # Celsius - saturation point
    WIND_STAGNATION_THRESHOLD = 8.0  # km/h - air stagnation
    FOG_HOURS_START = 8   # HE08 (8 AM local)
    FOG_HOURS_END = 13    # HE13 (1 PM local)
    FOG_SOLAR_PENALTY = 0.15  # 85% reduction in solar

    # Smoke thresholds (PM2.5 ug/m3)
    # Based on NREL/solar studies: 100ug/m3 ~= 13-15% loss
    # Higher concentrations can block up to 50% ("Orange Sky" events)
    SMOKE_TIERS = [
        (25, 1.00),   # Clean/Good - no penalty
        (50, 0.95),   # Moderate - 5% loss (slight haze)
        (100, 0.85),  # Unhealthy for Sensitive - 15% loss
        (200, 0.70),  # Unhealthy - 30% loss (visible smoke)
        (999, 0.50)   # Hazardous - 50% loss ("Orange Sky")
    ]

    def __init__(self):
        """Initialize the UncannyEngine."""
        self.timezone = ZoneInfo("America/Los_Angeles")
        logger.debug("[UncannyEngine] Initialized with Fog Guard + Smoke Guard")

    def normalize_temps(
        self,
        om_data: Dict[str, Any],
        nws_data: Optional[List[Dict[str, Any]]],
        met_no_data: Optional[List[Dict[str, Any]]],
        smoke_data: Optional[List[Dict[str, Any]]] = None
    ) -> pd.DataFrame:
        """
        Merge temperature and smoke data from all sources into a consensus DataFrame.

        Uses Open-Meteo as the time index base (it has the cleanest hourly data).
        NWS, Met.no temps, and smoke data are mapped to nearest hours.

        Args:
            om_data: Open-Meteo forecast result
            nws_data: List of NWS temperature records
            met_no_data: List of Met.no temperature records
            smoke_data: List of smoke/AQI records from SmokeProvider

        Returns:
            DataFrame with consensus temperatures, smoke data, and all source temps
        """
        logger.info("[UncannyEngine] Building consensus temperature model...")

        # Build base DataFrame from Open-Meteo (primary source)
        hourly = om_data.get('daily_summary', [])

        if not hourly:
            logger.error("[UncannyEngine] No hourly data in Open-Meteo result")
            raise ValueError("No hourly data available")

        df = pd.DataFrame(hourly)
        df['time'] = pd.to_datetime(df['time'])
        df = df.rename(columns={'temperature_c': 'temp_om'})

        logger.info(f"[UncannyEngine] Base data: {len(df)} hours from Open-Meteo")

        # Add NWS temperatures (if available)
        df['temp_nws'] = np.nan
        if nws_data:
            nws_df = pd.DataFrame(nws_data)
            nws_df['time'] = pd.to_datetime(nws_df['time'], utc=True)
            # Convert to local timezone for matching
            nws_df['time'] = nws_df['time'].dt.tz_convert(self.timezone).dt.tz_localize(None)

            # Merge to nearest hour
            nws_merged = 0
            for idx, row in df.iterrows():
                target_time = row['time']
                # Find closest NWS record within 2 hours
                time_diffs = abs(nws_df['time'] - target_time)
                if len(time_diffs) > 0:
                    closest_idx = time_diffs.idxmin()
                    if time_diffs[closest_idx] <= timedelta(hours=2):
                        df.at[idx, 'temp_nws'] = nws_df.at[closest_idx, 'temp_c']
                        nws_merged += 1

            logger.info(f"[UncannyEngine] Merged {nws_merged} NWS temperature records")
        else:
            logger.warning("[UncannyEngine] No NWS data available, using Open-Meteo only")

        # Add Met.no temperatures (if available)
        df['temp_met'] = np.nan
        if met_no_data:
            met_df = pd.DataFrame(met_no_data)
            met_df['time'] = pd.to_datetime(met_df['time'], utc=True)
            met_df['time'] = met_df['time'].dt.tz_convert(self.timezone).dt.tz_localize(None)

            met_merged = 0
            for idx, row in df.iterrows():
                target_time = row['time']
                time_diffs = abs(met_df['time'] - target_time)
                if len(time_diffs) > 0:
                    closest_idx = time_diffs.idxmin()
                    if time_diffs[closest_idx] <= timedelta(hours=2):
                        df.at[idx, 'temp_met'] = met_df.at[closest_idx, 'temp_c']
                        met_merged += 1

            logger.info(f"[UncannyEngine] Merged {met_merged} Met.no temperature records")
        else:
            logger.warning("[UncannyEngine] No Met.no data available")

        # Calculate CONSENSUS TEMPERATURE (average of available sources)
        temp_cols = ['temp_om', 'temp_nws', 'temp_met']
        df['temp_consensus'] = df[temp_cols].mean(axis=1, skipna=True)

        # Round for cleaner output
        df['temp_consensus'] = df['temp_consensus'].round(1)

        # Log source coverage
        sources_used = 1  # Always have Open-Meteo
        if df['temp_nws'].notna().any():
            sources_used += 1
        if df['temp_met'].notna().any():
            sources_used += 1

        logger.info(f"[UncannyEngine] Consensus model using {sources_used}/3 temperature sources")

        # --- NEW: SMOKE DATA MERGE ---
        df['pm2_5'] = 0.0
        df['us_aqi'] = 0
        
        if smoke_data:
            smoke_df = pd.DataFrame(smoke_data)
            smoke_df['time'] = pd.to_datetime(smoke_df['time'])
            
            smoke_merged = 0
            for idx, row in df.iterrows():
                target_time = row['time']
                matches = smoke_df[smoke_df['time'] == target_time]
                if not matches.empty:
                    df.at[idx, 'pm2_5'] = matches.iloc[0]['pm2_5']
                    df.at[idx, 'us_aqi'] = matches.iloc[0]['us_aqi']
                    smoke_merged += 1
            
            # Log smoke data summary
            max_pm = df['pm2_5'].max()
            avg_pm = df['pm2_5'].mean()
            logger.info(f"[UncannyEngine] Merged {smoke_merged} smoke records "
                       f"(Max PM2.5: {max_pm:.1f}, Avg: {avg_pm:.1f} ug/m3)")
            
            if max_pm > 100:
                logger.warning(f"[UncannyEngine] SMOKE ALERT: PM2.5 > 100 ug/m3 detected!")
            elif max_pm > 50:
                logger.info(f"[UncannyEngine] Moderate smoke levels detected (PM2.5 > 50)")
        else:
            logger.info("[UncannyEngine] No smoke data available - assuming clear air")

        return df

    def _get_smoke_penalty(self, pm2_5: float) -> float:
        """
        Calculate the smoke penalty multiplier based on PM2.5 concentration.
        
        Args:
            pm2_5: PM2.5 concentration in ug/m3
            
        Returns:
            Multiplier between 0.5 and 1.0 (1.0 = no penalty)
        """
        for limit, factor in self.SMOKE_TIERS:
            if pm2_5 <= limit:
                logger.debug(f"[UncannyEngine] PM2.5={pm2_5:.1f} -> smoke_penalty={factor}")
                return factor
        return 0.50  # Default to maximum penalty if above all thresholds

    def analyze_duck_curve(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Tule Fog detection AND Smoke Guard to adjust solar forecasts.
        
        This is the core "Uncanny" logic that detects conditions commercial
        apps miss. It applies two layers of adjustment:
        
        1. SMOKE GUARD (applies 24/7):
           - Calculates PM2.5-based penalty
           - Applies to base radiation value
           
        2. FOG GUARD (applies during critical hours):
           - Calculates fog probability from dewpoint depression + wind
           - Detects "Pre-Dawn Lock-in" (4-8 AM inversion layer)
           - Applies fog penalty on top of smoke adjustment
        
        The final solar_adjusted value is the MINIMUM of:
        - Smoke-adjusted solar (radiation * smoke_penalty)
        - Fog-adjusted solar (radiation * fog_penalty)
        
        Args:
            df: DataFrame with consensus temps, smoke, and Open-Meteo data

        Returns:
            DataFrame with adjusted solar values, risk levels, and smoke penalties
        """
        logger.info("[UncannyEngine] Running Fog Guard + Smoke Guard analysis...")
        
        df['solar_adjusted'] = df['radiation'].copy()
        df['risk_level'] = "LOW"
        df['fog_probability'] = 0.0
        df['smoke_penalty'] = 1.0  # Default: no penalty
        
        # Track persistent fog state (if it locks in early, it stays)
        is_fog_locked_in = False
        fog_hours_detected = 0
        lock_in_hours = 0
        smoke_hours_detected = 0

        for idx, row in df.iterrows():
            hour = row['time'].hour
            
            # Reset lock at midnight (new day logic)
            if hour == 0:
                is_fog_locked_in = False
                logger.debug(f"[UncannyEngine] Reset fog lock-in state for new day: {row['time'].date()}")

            # === 1. SMOKE GUARD (Applies 24/7) ===
            pm = row.get('pm2_5', 0)
            smoke_factor = self._get_smoke_penalty(pm)
            df.at[idx, 'smoke_penalty'] = smoke_factor
            
            # Apply smoke penalty to base radiation
            # (Smoke happens regardless of fog - it's the "first layer")
            if smoke_factor < 1.0:
                df.at[idx, 'solar_adjusted'] = row['radiation'] * smoke_factor
                
                # Set risk level for significant smoke
                if pm > 100:
                    smoke_hours_detected += 1
                    # Only override risk_level if not already set by fog
                    if df.at[idx, 'risk_level'] == "LOW":
                        df.at[idx, 'risk_level'] = f"SMOKE ({int(pm)} ug/m3)"
                        logger.debug(f"[UncannyEngine] SMOKE at {row['time']}: "
                                   f"PM2.5={pm:.1f}, penalty={smoke_factor:.2f}")

            # === 2. FOG GUARD (Calculated ON TOP of smoke) ===
            dewpoint = row.get('dewpoint_c', row.get('dewpoint', 0))
            wind = row.get('wind_speed_kmh', row.get('wind', 0))
            temp = row['temp_consensus']

            # Dew Point Depression
            dp_depression = temp - dewpoint

            # Physics Check (The Ingredients)
            is_saturated = dp_depression < self.DEW_POINT_DEPRESSION_THRESHOLD
            is_stagnant = wind < self.WIND_STAGNATION_THRESHOLD
            
            # 2a. CALCULATE PROBABILITY (All 24 Hours)
            # Fog forms when air is cold/trapped, regardless of sun
            depression_factor = max(0, 1 - (dp_depression / self.DEW_POINT_DEPRESSION_THRESHOLD))
            stagnation_factor = max(0, 1 - (wind / self.WIND_STAGNATION_THRESHOLD))
            fog_prob = round(depression_factor * stagnation_factor, 2)
            df.at[idx, 'fog_probability'] = fog_prob

            # 2b. CHECK FOR PRE-DAWN LOCK (Crucial for Tule Fog)
            # If we see heavy saturation/stagnation between 4AM-8AM, the "lid" is on.
            if 4 <= hour < 8 and fog_prob > 0.8:
                is_fog_locked_in = True
                lock_in_hours += 1
                logger.warning(f"[UncannyEngine] PRE-DAWN LOCK at {row['time']}: "
                             f"fog_prob={fog_prob:.2f}, dp_depression={dp_depression:.1f}C, "
                             f"wind={wind:.1f}km/h - INVERSION LAYER DETECTED")
            
            # 2c. APPLY FOG PENALTY (During Sun Hours)
            # Fog is MUCH WORSE than smoke - when both exist, fog dominates
            is_sun_up = self.FOG_HOURS_START <= hour <= self.FOG_HOURS_END
            
            if is_sun_up:
                current_adj = df.at[idx, 'solar_adjusted']  # Already has smoke penalty
                
                # Scenario A: Active conditions right now
                if is_saturated and is_stagnant:
                    fog_solar = row['radiation'] * self.FOG_SOLAR_PENALTY
                    
                    # Take the MINIMUM of smoke-adjusted vs fog-adjusted
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "CRITICAL (ACTIVE FOG)"
                        fog_hours_detected += 1
                        logger.debug(f"[UncannyEngine] ACTIVE FOG at {row['time']}: "
                                   f"dp_depression={dp_depression:.1f}C, wind={wind:.1f}km/h")
                
                # Scenario B: It cleared slightly, but we are "Locked In" from morning
                elif is_fog_locked_in:
                    # Even if temp rises slightly, the fog deck (stratus) often remains
                    fog_solar = row['radiation'] * 0.40  # 60% penalty
                    
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "HIGH (PERSISTENT STRATUS)"
                        logger.debug(f"[UncannyEngine] PERSISTENT STRATUS at {row['time']}: "
                                   f"Lock-in from pre-dawn, fog_prob={fog_prob:.2f}")
                
                # Scenario C: Watch/Warning
                elif fog_prob > 0.5:
                    fog_solar = row['radiation'] * 0.7
                    if fog_solar < current_adj:
                        df.at[idx, 'solar_adjusted'] = fog_solar
                        df.at[idx, 'risk_level'] = "MODERATE (RISK)"

        # Final summary logging
        if lock_in_hours > 0:
            logger.warning(f"[UncannyEngine] FOG LOCK-IN DETECTED: {lock_in_hours} pre-dawn hours triggered inversion layer")
        if fog_hours_detected > 0:
            logger.warning(f"[UncannyEngine] ACTIVE FOG: {fog_hours_detected} daytime hours flagged as CRITICAL")
        if smoke_hours_detected > 0:
            logger.warning(f"[UncannyEngine] SMOKE IMPACT: {smoke_hours_detected} hours with PM2.5 > 100 ug/m3")
        
        if not is_fog_locked_in and fog_hours_detected == 0 and smoke_hours_detected == 0:
            logger.info("[UncannyEngine] No Tule Fog or significant smoke detected - Clear forecast")

        return df

    def get_daily_summary(self, df: pd.DataFrame, days: int = 8) -> List[Dict[str, Any]]:
        """
        Generate daily temperature and smoke summary from analyzed data.

        Args:
            df: Analyzed DataFrame
            days: Number of days to summarize

        Returns:
            List of daily summaries with consensus temps and smoke data
        """
        df_copy = df.copy()
        df_copy['date'] = df_copy['time'].dt.date

        daily = df_copy.groupby('date').agg({
            'temp_consensus': 'mean',
            'temp_om': 'mean',
            'radiation': 'mean',
            'solar_adjusted': 'mean',
            'cloud_cover': 'mean',
            'pm2_5': 'max',          # Max PM2.5 for the day
            'smoke_penalty': 'min'   # Worst (lowest) smoke penalty for the day
        }).reset_index()

        daily = daily.head(days)

        summaries = []
        for _, row in daily.iterrows():
            summaries.append({
                "date": str(row['date']),
                "temp_consensus_c": round(row['temp_consensus'], 1),
                "temp_om_c": round(row['temp_om'], 1),
                "avg_solar_raw": round(row['radiation'], 0),
                "avg_solar_adjusted": round(row['solar_adjusted'], 0),
                "avg_cloud_cover": round(row['cloud_cover'], 0),
                "max_pm2_5": round(row['pm2_5'], 1),
                "min_smoke_penalty": round(row['smoke_penalty'], 2)
            })

        return summaries

    def get_duck_curve_hours(
        self,
        df: pd.DataFrame,
        target_date: Optional[datetime] = None
    ) -> List[AnalyzedHour]:
        """
        Extract HE09-HE16 (Duck Curve) hours for a specific date.

        Args:
            df: Analyzed DataFrame
            target_date: Date to extract (defaults to tomorrow)

        Returns:
            List of analyzed hours for the duck curve period, including smoke data
        """
        if target_date is None:
            target_date = datetime.now(self.timezone) + timedelta(days=1)
            target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # Filter to target date
        mask = df['time'].dt.date == target_date.date()
        day_df = df[mask]

        # Filter to duck curve hours (HE09-HE16 = 9 AM to 4 PM)
        duck_mask = day_df['time'].dt.hour.between(9, 16)
        duck_df = day_df[duck_mask]

        hours: List[AnalyzedHour] = []
        for _, row in duck_df.iterrows():
            hours.append({
                "time": row['time'].strftime("%Y-%m-%dT%H:%M"),
                "temp_consensus": row['temp_consensus'],
                "temp_om": row['temp_om'],
                "temp_nws": row['temp_nws'] if pd.notna(row['temp_nws']) else None,
                "temp_met": row['temp_met'] if pd.notna(row['temp_met']) else None,
                "dewpoint": row.get('dewpoint_c', row.get('dewpoint', 0)),
                "wind_kmh": row.get('wind_speed_kmh', row.get('wind', 0)),
                "solar_raw": row['radiation'],
                "solar_adjusted": row['solar_adjusted'],
                "cloud_cover": int(row['cloud_cover']),
                "risk_level": row['risk_level'],
                "fog_probability": row['fog_probability'],
                "pm2_5": row.get('pm2_5', 0.0),
                "smoke_penalty": row.get('smoke_penalty', 1.0)
            })

        return hours


if __name__ == "__main__":
    # Test the engine with mock data including smoke
    import asyncio
    from duck_sun.providers.open_meteo import fetch_open_meteo
    from duck_sun.providers.nws import NWSProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.providers.smoke import SmokeProvider

    logging.basicConfig(level=logging.INFO)

    async def test():
        print("=== Testing Uncanny Engine with Smoke Guard ===\n")

        # Fetch data from all sources
        print("Fetching Open-Meteo...")
        om_data = await fetch_open_meteo(days=3)

        print("Fetching NWS...")
        nws = NWSProvider()
        nws_data = await nws.fetch_async()

        print("Fetching Met.no...")
        met = MetNoProvider()
        met_data = await met.fetch_async()

        print("Fetching Smoke data...")
        smoke = SmokeProvider()
        smoke_data = await smoke.fetch_async(days=3)

        # Run the engine
        engine = UncannyEngine()

        print("\nBuilding consensus model with smoke data...")
        df = engine.normalize_temps(om_data, nws_data, met_data, smoke_data)

        print("\nRunning Fog Guard + Smoke Guard analysis...")
        df_analyzed = engine.analyze_duck_curve(df)

        print("\n=== Daily Summary (with Smoke) ===")
        for day in engine.get_daily_summary(df_analyzed, days=3):
            smoke_indicator = f" [SMOKE: {day['max_pm2_5']:.0f} ug/m3]" if day['max_pm2_5'] > 50 else ""
            print(f"  {day['date']}: {day['temp_consensus_c']}C "
                  f"(solar: {day['avg_solar_adjusted']:.0f} W/m²){smoke_indicator}")

        print("\n=== Tomorrow's Duck Curve (with Smoke) ===")
        duck_hours = engine.get_duck_curve_hours(df_analyzed)
        for hour in duck_hours:
            smoke_note = f" [PM2.5: {hour['pm2_5']:.0f}]" if hour['pm2_5'] > 25 else ""
            print(f"  {hour['time'][-5:]}: {hour['solar_adjusted']:.0f} W/m² "
                  f"[{hour['risk_level']}]{smoke_note}")

    asyncio.run(test())
