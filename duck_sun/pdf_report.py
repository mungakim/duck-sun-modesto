"""
PDF Report Generator for Duck Sun Modesto

Creates a compact half-page daily weather report for Modesto, CA with:
- 8-day forecast grid from 3 data sources (Hi/Lo in one cell)
- 3-day solar production forecast (HE09-16) - condensed
- Bottom half left blank for handwritten notes

Uses fpdf2 for PDF generation.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

try:
    from fpdf import FPDF
    HAS_FPDF = True
except ImportError:
    HAS_FPDF = False
    FPDF = None

import pandas as pd

logger = logging.getLogger(__name__)


class DuckSunPDF(FPDF):
    """Custom PDF class for Modesto Weather reports."""
    
    def __init__(self):
        super().__init__(orientation='L', unit='mm', format='Letter')  # Landscape
        self.set_auto_page_break(auto=False)  # No auto page break - we control layout
        
    def header(self):
        pass
    
    def footer(self):
        self.set_y(-8)
        self.set_font('Helvetica', 'I', 7)
        self.set_text_color(100, 100, 100)
        self.cell(0, 4, f'Duck Sun Modesto | {datetime.now().strftime("%Y-%m-%d %H:%M")}', 0, 0, 'C')


def calculate_daily_stats_from_hourly(hourly_data: List[Dict], timezone: str = "America/Los_Angeles") -> Dict[str, Dict]:
    """Calculate daily high/low from hourly data."""
    daily_stats = {}
    tz = ZoneInfo(timezone)
    
    for record in hourly_data:
        time_str = record.get('time', '')
        temp_c = record.get('temp_c', record.get('temperature_c', None))
        
        if temp_c is None:
            continue
            
        try:
            if '+' in time_str or 'Z' in time_str:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                dt = dt.astimezone(tz)
            else:
                dt = datetime.fromisoformat(time_str)
            
            date_key = dt.strftime('%Y-%m-%d')
            
            if date_key not in daily_stats:
                daily_stats[date_key] = {'temps': [], 'day_name': dt.strftime('%a')}
            
            daily_stats[date_key]['temps'].append(float(temp_c))
            
        except:
            continue
    
    result = {}
    for date_key, data in daily_stats.items():
        temps = data['temps']
        if temps:
            high_c = max(temps)
            low_c = min(temps)
            result[date_key] = {
                'date': date_key,
                'day_name': data['day_name'],
                'high_f': round(high_c * 9/5 + 32),
                'low_f': round(low_c * 9/5 + 32)
            }
    
    return result


def get_condition_color(condition: str) -> tuple:
    """Get RGB color for weather condition."""
    condition_lower = condition.lower()
    
    if 'clear' in condition_lower:
        return (255, 248, 200)
    elif 'partly' in condition_lower or 'mostly clear' in condition_lower:
        return (200, 225, 255)
    elif 'overcast' in condition_lower or 'mostly cloudy' in condition_lower:
        return (200, 200, 200)
    elif 'fog' in condition_lower:
        return (220, 220, 220)
    elif 'rain' in condition_lower or 'drizzle' in condition_lower or 'shower' in condition_lower:
        return (180, 200, 230)
    else:
        return (235, 235, 235)


def get_solar_condition_text(risk_level: str, solar_value: float) -> str:
    """Convert risk level to intuitive short description."""
    risk_upper = risk_level.upper()
    
    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return "Fog"
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return "Haze"
    elif 'MODERATE' in risk_upper:
        return "Clouds"
    elif solar_value < 50:
        return "Overcast"
    elif solar_value < 150:
        return "Partial"
    else:
        return "Clear"


def get_solar_color(risk_level: str, solar_value: float) -> tuple:
    """Get cell color based on solar conditions."""
    risk_upper = risk_level.upper()
    
    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return (255, 180, 180)  # Light red
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return (255, 210, 160)  # Light orange
    elif 'MODERATE' in risk_upper:
        return (255, 255, 180)  # Light yellow
    elif solar_value < 50:
        return (220, 220, 220)  # Gray
    elif solar_value < 150:
        return (200, 230, 255)  # Light blue
    else:
        return (200, 255, 200)  # Light green


def generate_pdf_report(
    om_data: Dict[str, Any],
    nws_data: Optional[List[Dict]],
    met_data: Optional[List[Dict]],
    df_analyzed: pd.DataFrame,
    fog_critical_hours: int = 0,
    output_path: Optional[Path] = None
) -> Optional[Path]:
    """Generate a compact half-page PDF weather report for Modesto, CA."""
    
    if not HAS_FPDF:
        logger.error("[generate_pdf_report] fpdf2 not installed")
        return None
    
    logger.info("[generate_pdf_report] Starting PDF generation...")
    
    om_daily = om_data.get('daily_forecast', [])[:8]
    nws_daily = calculate_daily_stats_from_hourly(nws_data) if nws_data else {}
    met_daily = calculate_daily_stats_from_hourly(met_data) if met_data else {}
    
    pdf = DuckSunPDF()
    pdf.add_page()
    
    # Layout
    margin = 8
    usable_width = 279 - (2 * margin)
    
    # ===================
    # HEADER (compact)
    # ===================
    pdf.set_font('Helvetica', 'B', 14)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 6, 'MODESTO, CA - DAILY WEATHER FORECAST', 0, 1, 'C')
    
    today = datetime.now(ZoneInfo("America/Los_Angeles"))
    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(0, 4, f'{today.strftime("%A, %B %d, %Y")}', 0, 1, 'C')
    pdf.ln(2)
    
    # ===================
    # 8-DAY FORECAST TABLE (Hi/Lo split into separate cells)
    # ===================
    label_col = 24
    day_col = (usable_width - label_col) / 8
    half_col = day_col / 2  # Split each day into Hi and Lo columns
    row_h = 6
    
    # --- DAY ROW (spans 2 columns per day) ---
    pdf.set_fill_color(0, 60, 120)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.cell(label_col, row_h, 'DAY', 1, 0, 'C', fill=True)
    
    for i, day in enumerate(om_daily):
        label = "TODAY" if i == 0 else day.get('day_name', '')
        pdf.cell(day_col, row_h, label, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # --- DATE ROW (spans 2 columns per day) ---
    pdf.set_fill_color(70, 110, 160)
    pdf.set_font('Helvetica', '', 6)
    pdf.cell(label_col, row_h - 1, 'DATE', 1, 0, 'C', fill=True)
    
    for day in om_daily:
        date_str = day.get('date', '')
        try:
            dt = datetime.fromisoformat(date_str)
            date_short = dt.strftime('%m/%d')
        except:
            date_short = '--'
        pdf.cell(day_col, row_h - 1, date_short, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # --- CONDITION ROW (spans 2 columns per day) ---
    pdf.set_text_color(0, 0, 0)
    pdf.set_font('Helvetica', '', 5)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(label_col, row_h, 'CONDITION', 1, 0, 'C', fill=True)
    
    for day in om_daily:
        condition = day.get('condition', 'Unknown')
        r, g, b = get_condition_color(condition)
        pdf.set_fill_color(r, g, b)
        pdf.cell(day_col, row_h, condition, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # ===================
    # TEMPERATURE ROWS (Hi and Lo in separate half-width cells)
    # ===================
    
    def draw_source_row(label: str, fill_color: tuple, get_hi_lo_func):
        """Draw a single row with separate Hi and Lo cells."""
        pdf.set_fill_color(*fill_color)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font('Helvetica', 'B', 6)
        pdf.cell(label_col, row_h, label, 1, 0, 'C', fill=True)
        
        pdf.set_font('Helvetica', '', 7)
        for day in om_daily:
            date_key = day.get('date', '')
            hi, lo = get_hi_lo_func(day, date_key)
            
            # High temp cell
            if hi != 'N/A':
                pdf.cell(half_col, row_h, str(hi), 1, 0, 'C', fill=True)
            else:
                pdf.cell(half_col, row_h, "--", 1, 0, 'C', fill=True)
            
            # Low temp cell
            if lo != 'N/A':
                pdf.cell(half_col, row_h, str(lo), 1, 0, 'C', fill=True)
            else:
                pdf.cell(half_col, row_h, "--", 1, 0, 'C', fill=True)
        pdf.ln()
    
    # Open-Meteo
    draw_source_row('OPEN-METEO', (255, 235, 235),
        lambda d, k: (d.get('high_f', 'N/A'), d.get('low_f', 'N/A')))
    
    # NWS
    draw_source_row('NWS (US Gov)', (235, 245, 255),
        lambda d, k: (nws_daily.get(k, {}).get('high_f', 'N/A'), 
                      nws_daily.get(k, {}).get('low_f', 'N/A')))
    
    # Met.no
    draw_source_row('MET.NO (EU)', (235, 255, 235),
        lambda d, k: (met_daily.get(k, {}).get('high_f', 'N/A'),
                      met_daily.get(k, {}).get('low_f', 'N/A')))
    
    # --- AVERAGES ROW ---
    # Same border logic as source rows:
    # - Hi cells: Right border (intraday separator) + first day also gets left border
    # - Lo cells: No left/right borders (gap between days) + last day gets right border
    pdf.set_fill_color(255, 220, 100)
    pdf.set_font('Helvetica', 'B', 6)
    pdf.cell(label_col, row_h, 'AVERAGES', 1, 0, 'C', fill=True)
    
    pdf.set_font('Helvetica', 'B', 7)
    num_days = len(om_daily)
    for i, day in enumerate(om_daily):
        date_key = day.get('date', '')
        
        highs = [day.get('high_f')]
        lows = [day.get('low_f')]
        
        if nws_daily.get(date_key):
            highs.append(nws_daily[date_key].get('high_f'))
            lows.append(nws_daily[date_key].get('low_f'))
        if met_daily.get(date_key):
            highs.append(met_daily[date_key].get('high_f'))
            lows.append(met_daily[date_key].get('low_f'))
        
        highs = [h for h in highs if h and h != 'N/A']
        lows = [l for l in lows if l and l != 'N/A']
        
        # Hi cell borders: Right (intraday separator) + Top/Bottom
        # First day also needs Left border (table edge)
        if i == 0:
            hi_border = 1  # All borders (LTRB) for first day's Hi
        else:
            hi_border = 'TBR'  # Top, Bottom, Right (no left = gap from previous day's Lo)
        
        # Lo cell borders: Top/Bottom only (no left/right = gap between days)
        # Last day needs Right border (table edge)
        if i == num_days - 1:
            lo_border = 'TBR'  # Top, Bottom, Right (table edge)
        else:
            lo_border = 'TB'  # Top, Bottom only (no left/right = gap to next day)
        
        if highs and lows:
            avg_hi = round(sum(highs) / len(highs))
            avg_lo = round(sum(lows) / len(lows))
            pdf.cell(half_col, row_h, str(avg_hi), hi_border, 0, 'C', fill=True)
            pdf.cell(half_col, row_h, str(avg_lo), lo_border, 0, 'C', fill=True)
        else:
            pdf.cell(half_col, row_h, "--", hi_border, 0, 'C', fill=True)
            pdf.cell(half_col, row_h, "--", lo_border, 0, 'C', fill=True)
    pdf.ln()
    
    # --- PRECIP ROW (spans both Hi/Lo columns) ---
    pdf.set_fill_color(200, 220, 255)
    pdf.set_font('Helvetica', 'B', 6)
    pdf.cell(label_col, row_h, 'PRECIP %', 1, 0, 'C', fill=True)
    
    pdf.set_font('Helvetica', '', 7)
    for day in om_daily:
        precip = day.get('precip_prob', 0)
        # Span across both Hi and Lo columns
        pdf.cell(day_col, row_h, f"{precip}%", 1, 0, 'C', fill=True)
    pdf.ln()
    
    pdf.ln(3)
    
    # ===================
    # COMPACT 3-DAY SOLAR FORECAST
    # ===================
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 5, 'SOLAR FORECAST (9AM-4PM) - W/m² Irradiance', 0, 1, 'L')
    
    # Get 3 future days
    tz = ZoneInfo("America/Los_Angeles")
    future_dates = [(today + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 4)]
    
    # Extract duck curve hours
    duck_data = {d: [] for d in future_dates}
    for _, row in df_analyzed.iterrows():
        try:
            row_date = row['time'].strftime('%Y-%m-%d')
            row_hour = row['time'].hour
            if row_date in future_dates and 9 <= row_hour <= 16:
                duck_data[row_date].append({
                    'hour': row_hour,
                    'solar': row.get('solar_adjusted', row.get('radiation', 0)),
                    'risk': row.get('risk_level', 'LOW')
                })
        except:
            continue
    
    # Compact table: 3 days across, 8 hours + labels
    hour_labels = ['9AM', '10', '11', '12PM', '1', '2', '3', '4PM']
    
    # Calculate column widths for compact layout
    date_label_col = 18
    hour_col = (usable_width - date_label_col) / 8
    solar_row_h = 5
    
    # Header row with hour labels
    pdf.set_fill_color(0, 60, 120)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 6)
    pdf.cell(date_label_col, solar_row_h, 'DATE', 1, 0, 'C', fill=True)
    for hl in hour_labels:
        pdf.cell(hour_col, solar_row_h, hl, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # Data rows for each day
    for date_str in future_dates:
        try:
            dt = datetime.fromisoformat(date_str)
            day_label = dt.strftime('%a %m/%d')
        except:
            day_label = date_str
        
        hours = duck_data.get(date_str, [])
        hours_dict = {h['hour']: h for h in hours}
        
        # Row: Date label + solar values with condition colors
        pdf.set_fill_color(240, 240, 240)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font('Helvetica', 'B', 5)
        pdf.cell(date_label_col, solar_row_h * 2, day_label, 1, 0, 'C', fill=True)
        
        # Solar values with colors
        pdf.set_font('Helvetica', '', 6)
        x_start = pdf.get_x()
        y_start = pdf.get_y()
        
        for i in range(8):
            hour_num = 9 + i
            hour_data = hours_dict.get(hour_num, {'solar': 0, 'risk': 'LOW'})
            solar_val = hour_data['solar']
            risk = hour_data['risk']
            
            r, g, b = get_solar_color(risk, solar_val)
            pdf.set_fill_color(r, g, b)
            
            # Solar value
            pdf.set_xy(x_start + i * hour_col, y_start)
            pdf.cell(hour_col, solar_row_h, f"{solar_val:.0f}", 1, 0, 'C', fill=True)
            
            # Condition text below
            condition_text = get_solar_condition_text(risk, solar_val)
            pdf.set_xy(x_start + i * hour_col, y_start + solar_row_h)
            pdf.set_font('Helvetica', 'I', 5)
            pdf.cell(hour_col, solar_row_h, condition_text, 1, 0, 'C', fill=True)
            pdf.set_font('Helvetica', '', 6)
        
        pdf.ln(solar_row_h * 2)
    
    # ===================
    # COMPACT LEGEND
    # ===================
    pdf.ln(2)
    pdf.set_font('Helvetica', 'I', 6)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 3, 'Temps: Hi/Lo °F | Solar: Fog=0-15% | Haze=40% | Clouds=70% | Clear=100% output | W/m²=Watts per sq meter sunlight', 0, 1, 'L')
    
    # ===================
    # NOTES SECTION LINE
    # ===================
    pdf.ln(3)
    pdf.set_draw_color(180, 180, 180)
    pdf.line(margin, pdf.get_y(), 279 - margin, pdf.get_y())
    pdf.ln(2)
    pdf.set_font('Helvetica', 'I', 8)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 4, 'NOTES:', 0, 1, 'L')
    
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
        logger.error(f"[generate_pdf_report] Failed to save PDF: {e}")
        return None


if __name__ == "__main__":
    import asyncio
    from duck_sun.providers.open_meteo import fetch_open_meteo
    from duck_sun.providers.nws import NWSProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.uncanniness import UncannyEngine
    
    logging.basicConfig(level=logging.INFO)
    
    async def test():
        print("=== Testing PDF Report Generator ===\n")
        
        om_data = await fetch_open_meteo(days=8)
        nws = NWSProvider()
        nws_data = await nws.fetch_async()
        met = MetNoProvider()
        met_data = await met.fetch_async()
        
        engine = UncannyEngine()
        df = engine.normalize_temps(om_data, nws_data, met_data)
        df_analyzed = engine.analyze_duck_curve(df)
        
        critical = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])
        
        pdf_path = generate_pdf_report(
            om_data=om_data,
            nws_data=nws_data,
            met_data=met_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical
        )
        
        if pdf_path:
            print(f"\n✅ PDF generated: {pdf_path}")
    
    asyncio.run(test())
