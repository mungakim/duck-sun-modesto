"""
Uncanny Engine for Duck Sun Modesto

Architecture:
1. Thermodynamics: NWS (Primary) > Met.no (Secondary) >> Open-Meteo (Fallback)
2. Energy: Open-Meteo (Physics)
3. Logic Override: NWS Text Narratives ("Dense Fog") force the model's hand.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class UncannyEngine:
    """
    The Hybrid Architecture Engine with Weighted Consensus.
    
    Temperature priority: NWS > Met.no > Open-Meteo
    Solar physics: Always Open-Meteo radiation data
    Logic override: NWS text narratives trigger fog probability boosts
    """
    
    DEW_POINT_DEPRESSION_THRESHOLD = 2.5
    WIND_STAGNATION_THRESHOLD = 8.0
    FOG_HOURS_START = 8   # HE08
    FOG_HOURS_END = 13    # HE13
    FOG_SOLAR_PENALTY = 0.15

    SMOKE_TIERS = [(25, 1.00), (50, 0.95), (100, 0.85), (200, 0.70), (999, 0.50)]

    def __init__(self):
        logger.info("[UncannyEngine] Initializing Hybrid Architecture engine...")
        self.timezone = ZoneInfo("America/Los_Angeles")

    def normalize_temps(
        self,
        om_data: Dict[str, Any],
        nws_data: Optional[List[Dict]],
        met_no_data: Optional[List[Dict]],
        smoke_data: Optional[List[Dict]] = None
    ) -> pd.DataFrame:
        """Merge temps using Weighted Consensus strategy: NWS > Met.no > Open-Meteo."""
        logger.info("[UncannyEngine] Building consensus model...")

        hourly = om_data.get('daily_summary', [])
        if not hourly:
            logger.error("[UncannyEngine] No hourly data in Open-Meteo result")
            raise ValueError("No hourly data")

        df = pd.DataFrame(hourly)
        df['time'] = pd.to_datetime(df['time'])
        df = df.rename(columns={'temperature_c': 'temp_om'})
        
        logger.info(f"[UncannyEngine] Base data: {len(df)} hours from Open-Meteo")

        # Merge NWS temperatures
        df['temp_nws'] = np.nan
        if nws_data:
            nws_df = pd.DataFrame(nws_data)
            nws_df['time'] = pd.to_datetime(nws_df['time'], utc=True).dt.tz_convert(self.timezone).dt.tz_localize(None)
            
            nws_merged = 0
            for idx, row in df.iterrows():
                matches = nws_df[(nws_df['time'] >= row['time'] - timedelta(minutes=30)) & 
                                 (nws_df['time'] <= row['time'] + timedelta(minutes=30))]
                if not matches.empty:
                    df.at[idx, 'temp_nws'] = matches.iloc[0]['temp_c']
                    nws_merged += 1
            
            logger.info(f"[UncannyEngine] Merged {nws_merged} NWS temperature records")
        else:
            logger.warning("[UncannyEngine] No NWS data available")

        # Merge Met.no temperatures
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

        # APPLY WEIGHTED LOGIC: NWS > Met.no > Open-Meteo
        df['temp_consensus'] = df['temp_om'].copy()  # Default fallback
        
        nws_used = 0
        met_used = 0
        om_used = 0
        
        for idx, row in df.iterrows():
            if pd.notna(row['temp_nws']):
                df.at[idx, 'temp_consensus'] = row['temp_nws']
                nws_used += 1
            elif pd.notna(row['temp_met']):
                df.at[idx, 'temp_consensus'] = row['temp_met']
                met_used += 1
            else:
                om_used += 1

        logger.info(f"[UncannyEngine] Consensus sources: NWS={nws_used}, Met.no={met_used}, OM={om_used}")

        # Merge Smoke data
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

    def analyze_duck_curve(
        self, 
        df: pd.DataFrame, 
        nws_text_data: Optional[List[Dict]] = None
    ) -> pd.DataFrame:
        """Apply Physics + Narrative Override for fog/smoke detection."""
        logger.info("[UncannyEngine] Running Fog Guard + Smoke Guard analysis...")
        
        df['solar_adjusted'] = df['radiation'].copy()
        df['risk_level'] = "LOW"
        df['fog_probability'] = 0.0
        df['smoke_penalty'] = 1.0
        
        # Check text forecast for fog keywords (Narrative Override)
        text_mentions_fog = False
        if nws_text_data:
            logger.info("[UncannyEngine] Scanning NWS text forecast for fog keywords...")
            for p in nws_text_data[:4]:  # Check next ~48 hours
                text = p.get('detailedForecast', '').lower()
                if 'dense fog' in text or 'patchy fog' in text or 'areas of fog' in text:
                    text_mentions_fog = True
                    logger.warning(f"[UncannyEngine] NARRATIVE OVERRIDE: '{p['name']}' mentions fog!")
                    break
        
        is_fog_locked_in = False
        fog_hours_detected = 0
        lock_in_hours = 0
        smoke_hours_detected = 0
        
        for idx, row in df.iterrows():
            hour = row['time'].hour
            
            # Reset lock at midnight (new day logic)
            if hour == 0:
                is_fog_locked_in = False

            # === 1. SMOKE GUARD (Applies 24/7) ===
            pm = row.get('pm2_5', 0)
            smoke_factor = 1.0
            for limit, factor in self.SMOKE_TIERS:
                if pm <= limit:
                    smoke_factor = factor
                    break
            df.at[idx, 'smoke_penalty'] = smoke_factor
            
            if smoke_factor < 1.0:
                df.at[idx, 'solar_adjusted'] = row['radiation'] * smoke_factor
                if pm > 100:
                    smoke_hours_detected += 1
                    df.at[idx, 'risk_level'] = f"SMOKE ({int(pm)} ug/m3)"

            # === 2. FOG GUARD ===
            dewpoint = row.get('dewpoint_c', row.get('dewpoint', 0))
            wind = row.get('wind_speed_kmh', row.get('wind', 0))
            
            dp_depression = row['temp_consensus'] - dewpoint
            
            # Calculate fog probability
            depression_factor = max(0, 1 - (dp_depression / self.DEW_POINT_DEPRESSION_THRESHOLD))
            stagnation_factor = max(0, 1 - (wind / self.WIND_STAGNATION_THRESHOLD))
            fog_prob = round(depression_factor * stagnation_factor, 2)
            
            # NARRATIVE OVERRIDE: If NWS text mentions fog, boost probability
            if text_mentions_fog and fog_prob > 0.3:
                fog_prob = min(0.99, fog_prob + 0.3)
                logger.debug(f"[UncannyEngine] Fog prob boosted by narrative: {fog_prob:.2f}")
            
            df.at[idx, 'fog_probability'] = fog_prob

            # Pre-Dawn Lock-In Check
            if 4 <= hour < 8 and fog_prob > 0.8:
                is_fog_locked_in = True
                lock_in_hours += 1
                logger.warning(f"[UncannyEngine] PRE-DAWN LOCK at {row['time']}: fog_prob={fog_prob:.2f}")

            # Apply Fog Penalties during sun hours
            if self.FOG_HOURS_START <= hour <= self.FOG_HOURS_END:
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
        if lock_in_hours > 0:
            logger.warning(f"[UncannyEngine] FOG LOCK-IN: {lock_in_hours} pre-dawn hours triggered inversion")
        if fog_hours_detected > 0:
            logger.warning(f"[UncannyEngine] ACTIVE FOG: {fog_hours_detected} daytime hours CRITICAL")
        if smoke_hours_detected > 0:
            logger.warning(f"[UncannyEngine] SMOKE IMPACT: {smoke_hours_detected} hours with PM2.5 > 100")
        
        if not is_fog_locked_in and fog_hours_detected == 0 and smoke_hours_detected == 0:
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
    from duck_sun.providers.nws import NWSProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.providers.smoke import SmokeProvider

    logging.basicConfig(level=logging.INFO)

    async def test():
        print("=== Testing Uncanny Engine (Hybrid Architecture) ===\n")

        print("Fetching Open-Meteo...")
        om_data = await fetch_open_meteo(days=3)

        print("Fetching NWS...")
        nws = NWSProvider()
        nws_data = await nws.fetch_async()
        nws_text = await nws.fetch_text_forecast()

        print("Fetching Met.no...")
        met = MetNoProvider()
        met_data = await met.fetch_async()

        print("Fetching Smoke data...")
        smoke = SmokeProvider()
        smoke_data = await smoke.fetch_async(days=3)

        engine = UncannyEngine()

        print("\nBuilding weighted consensus model...")
        df = engine.normalize_temps(om_data, nws_data, met_data, smoke_data)

        print("\nRunning Fog Guard + Smoke Guard with Narrative Override...")
        df_analyzed = engine.analyze_duck_curve(df, nws_text_data=nws_text)

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
