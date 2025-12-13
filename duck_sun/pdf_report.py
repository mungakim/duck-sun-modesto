"""
PDF Report Generator for Duck Sun Modesto
Weights: NWS(5x), Accu(3x), Met(3x), OM(1x)
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo
import pandas as pd

try:
    from fpdf import FPDF
    HAS_FPDF = True
except ImportError:
    HAS_FPDF = False
    FPDF = None

logger = logging.getLogger(__name__)


class DuckSunPDF(FPDF):
    """Custom PDF class for Modesto Weather reports."""
    
    def __init__(self):
        super().__init__(orientation='L', unit='mm', format='Letter')
        self.set_auto_page_break(auto=False)
        logger.debug("[DuckSunPDF] PDF instance created (Landscape Letter)")
        
    def header(self):
        pass
    
    def footer(self):
        self.set_y(-8)
        self.set_font('Helvetica', 'I', 7)
        self.set_text_color(100, 100, 100)
        self.cell(0, 4, f'Duck Sun Modesto | {datetime.now().strftime("%Y-%m-%d %H:%M")}', 0, 0, 'C')


def calculate_daily_stats_from_hourly(hourly_data: List[Dict], timezone: str = "America/Los_Angeles") -> Dict:
    """Calculate daily high/low from hourly data."""
    logger.debug(f"[calculate_daily_stats] Processing {len(hourly_data) if hourly_data else 0} hourly records")
    
    daily_stats = {}
    tz = ZoneInfo(timezone)
    
    for record in hourly_data:
        try:
            t = record.get('time', '')
            val = record.get('temp_c', record.get('temperature_c'))
            if val is None:
                continue
            
            if '+' in t or 'Z' in t:
                dt = datetime.fromisoformat(t.replace('Z', '+00:00')).astimezone(tz)
            else:
                dt = datetime.fromisoformat(t)
            
            k = dt.strftime('%Y-%m-%d')
            if k not in daily_stats:
                daily_stats[k] = {'temps': [], 'day_name': dt.strftime('%a')}
            daily_stats[k]['temps'].append(float(val))
            
        except Exception as e:
            logger.debug(f"[calculate_daily_stats] Failed to parse record: {e}")
            continue
    
    result = {}
    for k, d in daily_stats.items():
        if d['temps']:
            result[k] = {
                'date': k,
                'day_name': d['day_name'],
                'high_f': round(max(d['temps']) * 1.8 + 32),
                'low_f': round(min(d['temps']) * 1.8 + 32)
            }
    
    logger.debug(f"[calculate_daily_stats] Calculated stats for {len(result)} days")
    return result


def calculate_weighted_average(values: List[Optional[float]], weights: List[float]) -> Optional[int]:
    """Calculate weighted average from values with weights."""
    total_val, total_weight = 0.0, 0.0
    
    for val, weight in zip(values, weights):
        if val is not None:
            total_val += val * weight
            total_weight += weight
    
    if total_weight > 0:
        result = round(total_val / total_weight)
        logger.debug(f"[weighted_average] values={values}, weights={weights}, result={result}")
        return result
    return None


def get_rank_color(rank: int) -> tuple:
    """Get RGB color for rank badge."""
    if rank == 1:
        return (255, 215, 0)    # Gold
    elif rank == 2:
        return (192, 192, 192)  # Silver
    elif rank == 3:
        return (205, 127, 50)   # Bronze
    return (240, 240, 240)      # Default gray


def get_solar_color(risk_level: str, solar_value: float) -> tuple:
    """Get cell color based on solar conditions."""
    risk_upper = risk_level.upper()
    
    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return (255, 180, 180)
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return (255, 210, 160)
    elif 'MODERATE' in risk_upper:
        return (255, 255, 180)
    elif solar_value < 50:
        return (220, 220, 220)
    elif solar_value < 150:
        return (200, 230, 255)
    else:
        return (200, 255, 200)


def get_descriptive_risk(risk_level: str) -> str:
    """Convert risk codes to human-readable 3-7 word descriptions."""
    risk_upper = risk_level.upper()
    
    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return "Dense fog, minimal solar"
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return "Stratus layer blocking"
    elif 'MODERATE' in risk_upper:
        return "Fog risk, reduced solar"
    elif 'SMOKE' in risk_upper:
        # Extract PM2.5 value if present
        import re
        match = re.search(r'(\d+)', risk_level)
        if match:
            pm_val = int(match.group(1))
            if pm_val > 100:
                return "Heavy smoke, poor air"
            else:
                return "Light smoke haze"
        return "Smoke affecting solar"
    else:
        # LOW risk
        return "Clear, good conditions"


def generate_pdf_report(
    om_data: Dict,
    nws_data: Optional[List],
    met_data: Optional[List],
    accu_data: Optional[List],
    df_analyzed: pd.DataFrame,
    fog_critical_hours: int = 0,
    output_path: Optional[Path] = None,
    source_rankings: Optional[Dict] = None
) -> Optional[Path]:
    """
    Generate PDF report with 4-source temperature grid and weighted consensus.
    
    Args:
        om_data: Open-Meteo forecast data
        nws_data: NWS hourly data
        met_data: Met.no hourly data
        accu_data: AccuWeather daily data (5-day forecast)
        df_analyzed: Analyzed dataframe with solar/fog data
        fog_critical_hours: Number of critical fog hours
        output_path: Output path for PDF
        source_rankings: Dict mapping source name to rank (1-3, 0=unranked)
    """
    
    if not HAS_FPDF:
        logger.error("[generate_pdf_report] fpdf2 not installed")
        return None
    
    logger.info("[generate_pdf_report] Starting PDF generation...")
    logger.info(f"[generate_pdf_report] Source rankings: {source_rankings}")
    logger.info(f"[generate_pdf_report] AccuWeather data: {len(accu_data) if accu_data else 0} days")
    
    source_rankings = source_rankings or {}
    
    # Process data sources
    om_daily = om_data.get('daily_forecast', [])[:8]
    nws_daily = calculate_daily_stats_from_hourly(nws_data) if nws_data else {}
    met_daily = calculate_daily_stats_from_hourly(met_data) if met_data else {}
    
    # Process AccuWeather data
    # Now uses native Fahrenheit values (no conversion rounding)
    accu_daily = {}
    if accu_data:
        for d in accu_data:
            # Use native F if available, else convert from C
            if 'high_f' in d and 'low_f' in d:
                accu_daily[d['date']] = {
                    'high_f': int(d['high_f']),  # Native F from API
                    'low_f': int(d['low_f'])     # Native F from API
                }
            else:
                # Fallback for old cache format
                accu_daily[d['date']] = {
                    'high_f': round(d['high_c'] * 1.8 + 32),
                    'low_f': round(d['low_c'] * 1.8 + 32)
                }
        logger.info(f"[generate_pdf_report] AccuWeather processed: {len(accu_daily)} days (native F)")

    pdf = DuckSunPDF()
    pdf.add_page()
    margin = 8
    usable_width = 279 - (2 * margin)
    
    # ===================
    # HEADER
    # ===================
    pdf.set_font('Helvetica', 'B', 14)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 6, 'MODESTO, CA - DAILY WEATHER FORECAST', 0, 1, 'C')
    
    today = datetime.now(ZoneInfo("America/Los_Angeles"))
    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 4, f'{today.strftime("%A, %B %d, %Y")}', 0, 1, 'C')
    pdf.ln(4)
    
    # ===================
    # TEMPERATURE GRID (4 Sources + Weighted Consensus)
    # ===================
    rank_col, source_col = 8, 20
    day_col = (usable_width - (rank_col + source_col)) / 8
    half_col, row_h = day_col / 2, 6
    
    logger.info("[generate_pdf_report] Drawing temperature grid...")
    
    # Header Row (Day Names)
    pdf.set_fill_color(0, 60, 120)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.cell(rank_col, row_h, 'RNK', 1, 0, 'C', 1)
    pdf.cell(source_col, row_h, 'SOURCE', 1, 0, 'C', 1)
    
    for i, day in enumerate(om_daily):
        label = "TODAY" if i == 0 else day.get('day_name', '')[:3].upper()
        pdf.cell(day_col, row_h, label, 1, 0, 'C', 1)
    pdf.ln()

    # Dates Row
    pdf.set_fill_color(70, 110, 160)
    pdf.set_font('Helvetica', '', 6)
    pdf.cell(rank_col, row_h-1, '', 1, 0, 'C', 1)
    pdf.cell(source_col, row_h-1, 'DATE', 1, 0, 'C', 1)
    for day in om_daily:
        date_str = day.get('date', '')[5:]  # MM-DD
        pdf.cell(day_col, row_h-1, date_str, 1, 0, 'C', 1)
    pdf.ln()

    def draw_row(label: str, fill: tuple, getter, rank_key: str = ""):
        """Draw a single row with rank badge + source name + Hi/Lo cells."""
        pdf.set_fill_color(*fill)
        pdf.set_text_color(0, 0, 0)
        
        # Rank badge
        rank = source_rankings.get(rank_key, 0)
        c = get_rank_color(rank)
        pdf.set_fill_color(*c)
        pdf.cell(rank_col, row_h, f"#{rank}" if rank else "", 1, 0, 'C', 1)
        
        # Source label
        pdf.set_fill_color(*fill)
        pdf.set_font('Helvetica', 'B', 6)
        pdf.cell(source_col, row_h, label, 1, 0, 'C', 1)
        
        # Temperature cells
        pdf.set_font('Helvetica', '', 7)
        for d in om_daily:
            v1, v2 = getter(d, d.get('date', ''))
            pdf.cell(half_col, row_h, str(v1) if v1 else "--", 1, 0, 'C', 1)
            pdf.cell(half_col, row_h, str(v2) if v2 else "--", 1, 0, 'C', 1)
        pdf.ln()

    # Draw source rows
    draw_row('OPEN-METEO', (255, 235, 235), 
             lambda d, k: (d.get('high_f'), d.get('low_f')), "Open-Meteo")
    
    draw_row('NWS (GOV)', (235, 245, 255), 
             lambda d, k: (nws_daily.get(k, {}).get('high_f'), nws_daily.get(k, {}).get('low_f')), "NWS")
    
    draw_row('MET.NO (EU)', (235, 255, 235), 
             lambda d, k: (met_daily.get(k, {}).get('high_f'), met_daily.get(k, {}).get('low_f')), "Met.no")
    
    draw_row('ACCU (COM)', (255, 245, 235), 
             lambda d, k: (accu_daily.get(k, {}).get('high_f'), accu_daily.get(k, {}).get('low_f')), "AccuWeather")

    # ===================
    # WEIGHTED CONSENSUS ROW
    # Weights: OM(1), NWS(5), Met(3), Accu(3)
    # ===================
    logger.info("[generate_pdf_report] Calculating weighted consensus...")
    
    pdf.set_fill_color(255, 220, 100)
    pdf.set_font('Helvetica', 'B', 6)
    pdf.cell(rank_col, row_h, '', 1, 0, 'C', 1)
    pdf.cell(source_col, row_h, 'CONSENSUS', 1, 0, 'C', 1)
    
    weights = [1.0, 5.0, 3.0, 3.0]  # Weights: OM, NWS, Met, Accu
    
    for day in om_daily:
        k = day.get('date', '')
        
        hi_vals = [
            day.get('high_f'),
            nws_daily.get(k, {}).get('high_f'),
            met_daily.get(k, {}).get('high_f'),
            accu_daily.get(k, {}).get('high_f')
        ]
        lo_vals = [
            day.get('low_f'),
            nws_daily.get(k, {}).get('low_f'),
            met_daily.get(k, {}).get('low_f'),
            accu_daily.get(k, {}).get('low_f')
        ]
        
        avg_hi = calculate_weighted_average(hi_vals, weights)
        avg_lo = calculate_weighted_average(lo_vals, weights)
        
        logger.debug(f"[generate_pdf_report] {k}: hi_vals={hi_vals}, avg_hi={avg_hi}")
        
        pdf.cell(half_col, row_h, str(avg_hi) if avg_hi else "--", 1, 0, 'C', 1)
        pdf.cell(half_col, row_h, str(avg_lo) if avg_lo else "--", 1, 0, 'C', 1)
    pdf.ln()
    
    # ===================
    # SOLAR FORECAST GRID (3-Day)
    # ===================
    pdf.ln(3)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 5, 'SOLAR FORECAST (9AM-4PM) - W/m² Irradiance', 0, 1, 'L')
    
    logger.info("[generate_pdf_report] Drawing solar forecast grid...")
    
    tz = ZoneInfo("America/Los_Angeles")
    future_dates = [(datetime.now(tz) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 4)]
    
    duck_data = {d: [] for d in future_dates}
    for _, row in df_analyzed.iterrows():
        try:
            row_date = row['time'].strftime('%Y-%m-%d')
            row_hour = row['time'].hour
            if row_date in future_dates and 9 <= row_hour <= 16:
                duck_data[row_date].append({
                    'hour': row_hour,
                    'solar': row.get('solar_adjusted', 0),
                    'risk': row.get('risk_level', 'LOW')
                })
        except Exception as e:
            logger.debug(f"[generate_pdf_report] Error processing row: {e}")
            continue

    date_label_col = 22
    hour_col = (usable_width - date_label_col) / 8
    solar_row_h = 5
    
    # Header row
    pdf.set_fill_color(0, 60, 120)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 6)
    pdf.cell(date_label_col, solar_row_h, 'DATE', 1, 0, 'C', 1)
    for hl in ['9AM', '10', '11', '12PM', '1', '2', '3', '4PM']:
        pdf.cell(hour_col, solar_row_h, hl, 1, 0, 'C', 1)
    pdf.ln()

    # Data rows
    for d in future_dates:
        # Save starting position for this row
        row_x_start = pdf.get_x()
        row_y_start = pdf.get_y()
        
        # Get day name for the date
        date_obj = datetime.strptime(d, '%Y-%m-%d')
        day_name = date_obj.strftime('%A')
        logger.debug(f"[generate_pdf_report] Solar grid date: {d} -> {d[5:]} {day_name}")
        
        # Draw single date cell spanning both rows with border
        pdf.set_fill_color(240, 240, 240)
        pdf.set_text_color(0, 0, 0)
        pdf.set_xy(row_x_start, row_y_start)
        pdf.cell(date_label_col, solar_row_h * 2, '', 1, 0, 'C', 1)  # Draw border + fill only
        
        # Now draw the text inside (no borders) - date on top, day name below
        # Use tight line height (3mm) and center both lines together in the 10mm cell
        text_line_h = 3
        text_block_h = text_line_h * 2  # Total height of both text lines
        y_offset = (solar_row_h * 2 - text_block_h) / 2  # Center the text block vertically
        
        pdf.set_font('Helvetica', 'B', 6)
        pdf.set_xy(row_x_start, row_y_start + y_offset)
        pdf.cell(date_label_col, text_line_h, d[5:], 0, 0, 'C', 0)  # Date text, no border
        pdf.set_xy(row_x_start, row_y_start + y_offset + text_line_h)
        pdf.cell(date_label_col, text_line_h, day_name, 0, 0, 'C', 0)  # Day text, no border
        
        # Set position for hourly cells (to the right of date column, at row start y)
        x_start = row_x_start + date_label_col
        y_start = row_y_start
        
        pdf.set_font('Helvetica', '', 6)
        hours_dict = {h['hour']: h for h in duck_data.get(d, [])}
        
        for i in range(8):
            h_data = hours_dict.get(9+i, {'solar': 0, 'risk': 'LOW'})
            r, g, b = get_solar_color(h_data['risk'], h_data['solar'])
            pdf.set_fill_color(r, g, b)
            
            # Solar value
            pdf.set_xy(x_start + i * hour_col, y_start)
            pdf.cell(hour_col, solar_row_h, f"{h_data['solar']:.0f}", 1, 0, 'C', 1)
            
            # Risk label - use descriptive text instead of truncated codes
            pdf.set_xy(x_start + i * hour_col, y_start + solar_row_h)
            pdf.set_font('Helvetica', 'I', 6)  # 6pt (2pts larger than original 4pt)
            risk_desc = get_descriptive_risk(h_data['risk'])
            pdf.cell(hour_col, solar_row_h, risk_desc, 1, 0, 'C', 1)
            pdf.set_font('Helvetica', '', 6)
        
        # Move to next row
        pdf.set_xy(row_x_start, row_y_start + solar_row_h * 2)

    # ===================
    # LEGEND
    # ===================
    pdf.ln(2)
    pdf.set_font('Helvetica', 'I', 6)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 3, 'Weights: NWS(5), Accu(3), Met(3), OM(1) | Temps: Hi/Lo °F | Solar: W/m² irradiance', 0, 1, 'L')
    
    
    # ===================
    # SAVE PDF
    # ===================
    if output_path is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = Path("reports") / f"daily_forecast_{timestamp}.pdf"
    
    output_path.parent.mkdir(exist_ok=True)
    
    try:
        pdf.output(str(output_path))
        logger.info(f"[generate_pdf_report] PDF saved to: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"[generate_pdf_report] Failed to save PDF: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    import asyncio
    from duck_sun.providers.open_meteo import fetch_open_meteo
    from duck_sun.providers.nws import NWSProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.providers.accuweather import AccuWeatherProvider
    from duck_sun.uncanniness import UncannyEngine
    from dotenv import load_dotenv
    
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    
    async def test():
        print("=== Testing PDF Report Generator (Hybrid Architecture) ===\n")
        
        om_data = await fetch_open_meteo(days=8)
        
        nws = NWSProvider()
        nws_data = await nws.fetch_async()
        
        met = MetNoProvider()
        met_data = await met.fetch_async()
        
        accu = AccuWeatherProvider()
        accu_data = await accu.fetch_forecast()
        
        engine = UncannyEngine()
        df = engine.normalize_temps(om_data, nws_data, met_data)
        df_analyzed = engine.analyze_duck_curve(df)
        
        critical = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
        
        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            accu_data=accu_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical,
            source_rankings={"Open-Meteo": 3, "NWS": 1, "Met.no": 2, "AccuWeather": 0}
        )
        
        if pdf_path:
            print(f"\n✅ PDF generated: {pdf_path}")
    
    asyncio.run(test())
