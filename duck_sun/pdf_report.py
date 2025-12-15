"""
PDF Report Generator for Duck Sun Modesto
Weights: NWS(5x), Accu(3x), Weather.com(2x), OM(1x)

WEIGHTED ENSEMBLE ARCHITECTURE - Reliability is King
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
        # No footer - removed per user request
        pass


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
    weathercom_data: Optional[List] = None,
    mid_data: Optional[Dict] = None,
    hrrr_data: Optional[Dict] = None,
    precip_data: Optional[Dict] = None,
    degraded_sources: Optional[List[str]] = None,
    nws_daily_periods: Optional[Dict] = None
) -> Optional[Path]:
    """
    Generate PDF report with 4-source temperature grid and weighted consensus.

    Args:
        om_data: Open-Meteo forecast data
        nws_data: NWS hourly data (fallback if period data unavailable)
        met_data: Met.no hourly data (kept for backward compat, not displayed)
        accu_data: AccuWeather daily data (5-day forecast)
        df_analyzed: Analyzed dataframe with solar/fog data
        fog_critical_hours: Number of critical fog hours
        output_path: Output path for PDF
        weathercom_data: Weather.com daily data (replaces Met.no in display)
        mid_data: MID.org 48-hour summary data
        hrrr_data: HRRR model data (48-hour, 3km resolution)
        precip_data: Aggregated precipitation probabilities by date
        degraded_sources: List of providers using cached/stale data
        nws_daily_periods: PRIORITY - NWS Period-based daily stats (matches website)
    """

    if not HAS_FPDF:
        logger.error("[generate_pdf_report] fpdf2 not installed")
        return None

    logger.info("[generate_pdf_report] Starting PDF generation...")
    logger.info(f"[generate_pdf_report] AccuWeather data: {len(accu_data) if accu_data else 0} days")
    
    # Process data sources
    om_daily = om_data.get('daily_forecast', [])[:8]
    met_daily = calculate_daily_stats_from_hourly(met_data) if met_data else {}

    # PRIORITY: Use NWS Period Data if available (matches website)
    if nws_daily_periods:
        logger.info("[generate_pdf_report] Using NWS Period Data (Website Match)")
        nws_daily = nws_daily_periods
    else:
        # Fallback to calculating from hourly grid (Legacy/Risk of mismatch)
        logger.info("[generate_pdf_report] Falling back to NWS hourly aggregation")
        nws_daily = calculate_daily_stats_from_hourly(nws_data) if nws_data else {}

    # Process Weather.com data (replaces Met.no in display)
    weathercom_daily = {}
    if weathercom_data:
        for d in weathercom_data:
            high_f = d.get('high_f')
            low_f = d.get('low_f')
            high_c = d.get('high_c')
            low_c = d.get('low_c')
            # Skip entries without valid high temp (e.g., "Tonight" forecast)
            if high_f is None and high_c is None:
                continue
            if high_f is not None and low_f is not None:
                weathercom_daily[d['date']] = {
                    'high_f': int(high_f),
                    'low_f': int(low_f)
                }
            elif high_c is not None and low_c is not None:
                weathercom_daily[d['date']] = {
                    'high_f': round(high_c * 1.8 + 32),
                    'low_f': round(low_c * 1.8 + 32)
                }
        logger.info(f"[generate_pdf_report] Weather.com processed: {len(weathercom_daily)} days")

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

    # Capture exact timestamp for report
    report_time = datetime.now(ZoneInfo("America/Los_Angeles"))
    timestamp_str = report_time.strftime("%A, %B %d, %Y %H:%M:%S")

    # ===================
    # HEADER with timestamp
    # ===================
    pdf.set_font('Helvetica', 'B', 14)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 6, 'MODESTO, CA - DAILY WEATHER FORECAST', 0, 1, 'C')

    # Date and timestamp (bigger, more prominent)
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 5, timestamp_str, 0, 1, 'C')
    pdf.ln(2)

    # ===================
    # DATA QUALITY WARNING BANNER (if degraded sources)
    # ===================
    if degraded_sources:
        pdf.set_fill_color(255, 230, 230)  # Light red background
        pdf.set_draw_color(200, 100, 100)
        pdf.set_text_color(139, 0, 0)  # Dark red text
        pdf.set_font('Helvetica', 'B', 7)
        warning_text = f"DATA QUALITY: {', '.join(degraded_sources)} using cached/stale data"
        pdf.cell(0, 5, warning_text, 1, 1, 'C', fill=True)
        pdf.set_text_color(0, 0, 0)  # Reset text color
        pdf.ln(1)

    # ===================
    # TOP ROW: Manual Entry Fields (left) + MID Weather 48-Hour Summary (right)
    # ===================
    top_row_y = pdf.get_y()

    # LEFT SIDE: MID GAS BURN - 3 rows of blank cells (Date | MMBtu)
    pdf.set_xy(margin, top_row_y)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(55, 4, 'MID GAS BURN:', 0, 1, 'L')

    # Draw 3 rows of cells: Date (1/3 width) | MMBtu value (2/3 width)
    cell_start_y = pdf.get_y()
    date_width = 18  # 1/3 of total
    value_width = 36  # 2/3 of total
    cell_height = 5

    pdf.set_font('Helvetica', '', 7)
    pdf.set_draw_color(100, 100, 100)

    for i in range(3):
        row_y = cell_start_y + (i * cell_height)
        # Date cell (left 1/3)
        pdf.set_xy(margin, row_y)
        pdf.cell(date_width, cell_height, '', 1, 0, 'C')  # Empty bordered cell
        # MMBtu value cell (right 2/3)
        pdf.set_xy(margin + date_width, row_y)
        pdf.cell(value_width, cell_height, '', 1, 0, 'C')  # Empty bordered cell

    # PGE CITYGATE with blank space for price
    citygate_y = cell_start_y + (3 * cell_height) + 2
    pdf.set_xy(margin, citygate_y)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.cell(35, 5, 'PGE CITYGATE:', 0, 0, 'L')
    # Blank box for price entry (fits ~6 chars like "4.305")
    pdf.set_xy(margin + 35, citygate_y)
    pdf.set_font('Helvetica', '', 8)
    pdf.cell(20, 5, '', 1, 0, 'C')  # Empty bordered cell for price

    # RIGHT SIDE: MID Weather 48-Hour Summary box with color-coded High/Low cells
    mid_box_x = usable_width - 70 + margin
    mid_box_width = 78
    mid_box_height = 26

    pdf.set_xy(mid_box_x, top_row_y)
    pdf.set_fill_color(240, 248, 255)  # Light blue background
    pdf.set_draw_color(0, 60, 120)
    pdf.rect(mid_box_x, top_row_y, mid_box_width, mid_box_height, 'DF')

    pdf.set_xy(mid_box_x + 2, top_row_y + 1)
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(mid_box_width - 4, 4, 'MID WEATHER 48-HOUR SUMMARY', 0, 1, 'C')

    # MID data display with color-coded High/Low cells
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(0, 0, 0)

    if mid_data:
        today_data = mid_data.get('today', {})
        yest_data = mid_data.get('yesterday', {})

        today_hi = today_data.get('high', '--')
        today_lo = today_data.get('low', '--')
        today_rain = today_data.get('rain', '0.00')
        yest_hi = yest_data.get('high', '--')
        yest_lo = yest_data.get('low', '--')
        yest_rain = yest_data.get('rain', '0.00')

        # Column headers: Label | High | Low | Rain
        header_y = top_row_y + 6
        label_x = mid_box_x + 2
        hi_x = mid_box_x + 22
        lo_x = mid_box_x + 34
        rain_x = mid_box_x + 46
        cell_w = 12
        cell_h = 4

        # Header row
        pdf.set_font('Helvetica', 'B', 6)
        pdf.set_xy(label_x, header_y)
        pdf.cell(20, cell_h, '', 0, 0, 'L')  # Empty label column header
        pdf.set_xy(hi_x, header_y)
        pdf.cell(cell_w, cell_h, 'High', 0, 0, 'C')
        pdf.set_xy(lo_x, header_y)
        pdf.cell(cell_w, cell_h, 'Low', 0, 0, 'C')
        pdf.set_xy(rain_x, header_y)
        pdf.cell(cell_w + 8, cell_h, 'Rain', 0, 0, 'C')

        # TODAY row
        row1_y = header_y + cell_h
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_xy(label_x, row1_y)
        pdf.cell(20, cell_h, 'TODAY', 0, 0, 'L')

        pdf.set_font('Helvetica', '', 7)
        # High cell - warm red/orange background
        pdf.set_fill_color(255, 200, 180)
        pdf.set_xy(hi_x, row1_y)
        pdf.cell(cell_w, cell_h, f'{today_hi}F', 1, 0, 'C', fill=True)
        # Low cell - cool blue background
        pdf.set_fill_color(180, 210, 255)
        pdf.set_xy(lo_x, row1_y)
        pdf.cell(cell_w, cell_h, f'{today_lo}F', 1, 0, 'C', fill=True)
        # Rain
        pdf.set_fill_color(255, 255, 255)
        pdf.set_xy(rain_x, row1_y)
        pdf.cell(cell_w + 8, cell_h, f'{today_rain}"', 0, 0, 'C')

        # YESTERDAY row
        row2_y = row1_y + cell_h
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_xy(label_x, row2_y)
        pdf.cell(20, cell_h, 'YEST', 0, 0, 'L')

        pdf.set_font('Helvetica', '', 7)
        # High cell - warm red/orange background
        pdf.set_fill_color(255, 200, 180)
        pdf.set_xy(hi_x, row2_y)
        pdf.cell(cell_w, cell_h, f'{yest_hi}F', 1, 0, 'C', fill=True)
        # Low cell - cool blue background
        pdf.set_fill_color(180, 210, 255)
        pdf.set_xy(lo_x, row2_y)
        pdf.cell(cell_w, cell_h, f'{yest_lo}F', 1, 0, 'C', fill=True)
        # Rain
        pdf.set_fill_color(255, 255, 255)
        pdf.set_xy(rain_x, row2_y)
        pdf.cell(cell_w + 8, cell_h, f'{yest_rain}"', 0, 0, 'C')

        # Historical records row (if available)
        if 'record_high_temp' in mid_data:
            row3_y = row2_y + cell_h + 1
            pdf.set_xy(mid_box_x + 2, row3_y)
            pdf.set_font('Helvetica', 'I', 6)
            rec_hi = mid_data.get('record_high_temp', '--')
            rec_hi_yr = mid_data.get('record_high_year', '')
            rec_lo = mid_data.get('record_low_temp', '--')
            rec_lo_yr = mid_data.get('record_low_year', '')
            pdf.cell(74, 3, f'Records: Hi {rec_hi}F({rec_hi_yr}) Lo {rec_lo}F({rec_lo_yr})', 0, 0, 'L')
    else:
        pdf.set_xy(mid_box_x + 2, top_row_y + 10)
        pdf.cell(74, 4, 'Data unavailable', 0, 0, 'C')

    # Move below the top row
    pdf.set_y(top_row_y + mid_box_height + 2)
    pdf.ln(2)
    
    # ===================
    # TEMPERATURE GRID (4 Sources + Weighted Consensus)
    # Color-coded day columns for easy reading
    # ===================
    source_col = 22
    day_col = (usable_width - source_col) / 8
    half_col, row_h = day_col / 2, 6

    # Define alternating day column colors (pastels for readability)
    DAY_COLORS = [
        (255, 240, 240),  # Day 0: Light pink
        (240, 255, 240),  # Day 1: Light green
        (240, 248, 255),  # Day 2: Light blue
        (255, 255, 240),  # Day 3: Light yellow
        (255, 245, 238),  # Day 4: Light peach
        (245, 255, 250),  # Day 5: Mint cream
        (248, 248, 255),  # Day 6: Ghost white
        (255, 250, 240),  # Day 7: Floral white
    ]

    logger.info("[generate_pdf_report] Drawing temperature grid...")

    # Header Row (Day Names) - Color coded by day
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_fill_color(0, 60, 120)
    pdf.cell(source_col, row_h, 'SOURCE', 1, 0, 'C', 1)

    for i, day in enumerate(om_daily):
        label = "TODAY" if i == 0 else day.get('day_name', '')[:3].upper()
        # Darker version of day color for header
        base_color = DAY_COLORS[i % len(DAY_COLORS)]
        dark_color = (max(0, base_color[0] - 100), max(0, base_color[1] - 80), max(0, base_color[2] - 60))
        pdf.set_fill_color(*dark_color)
        pdf.cell(day_col, row_h, label, 1, 0, 'C', 1)
    pdf.ln()

    # Dates Row - Color coded
    pdf.set_font('Helvetica', '', 6)
    pdf.set_fill_color(70, 110, 160)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(source_col, row_h-1, 'DATE', 1, 0, 'C', 1)
    for i, day in enumerate(om_daily):
        date_str = day.get('date', '')[5:]  # MM-DD
        base_color = DAY_COLORS[i % len(DAY_COLORS)]
        dark_color = (max(0, base_color[0] - 70), max(0, base_color[1] - 50), max(0, base_color[2] - 30))
        pdf.set_fill_color(*dark_color)
        pdf.cell(day_col, row_h-1, date_str, 1, 0, 'C', 1)
    pdf.ln()

    def draw_row_colored(label: str, getter):
        """Draw a single row with source name + color-coded Hi/Lo cells."""
        pdf.set_text_color(0, 0, 0)

        # Source label (neutral gray)
        pdf.set_fill_color(245, 245, 245)
        pdf.set_font('Helvetica', 'B', 6)
        pdf.cell(source_col, row_h, label, 1, 0, 'C', 1)

        # Temperature cells - COLOR CODED BY DAY
        pdf.set_font('Helvetica', '', 7)
        for i, d in enumerate(om_daily):
            day_color = DAY_COLORS[i % len(DAY_COLORS)]
            pdf.set_fill_color(*day_color)
            v1, v2 = getter(d, d.get('date', ''))
            pdf.cell(half_col, row_h, str(v1) if v1 else "--", 1, 0, 'C', 1)
            pdf.cell(half_col, row_h, str(v2) if v2 else "--", 1, 0, 'C', 1)
        pdf.ln()

    # Draw source rows with day-colored columns
    draw_row_colored('OPEN-METEO',
             lambda d, k: (d.get('high_f'), d.get('low_f')))

    draw_row_colored('NWS (GOV)',
             lambda d, k: (nws_daily.get(k, {}).get('high_f'), nws_daily.get(k, {}).get('low_f')))

    draw_row_colored('WEATHER.COM',
             lambda d, k: (weathercom_daily.get(k, {}).get('high_f'), weathercom_daily.get(k, {}).get('low_f')))

    draw_row_colored('ACCU (COM)',
             lambda d, k: (accu_daily.get(k, {}).get('high_f'), accu_daily.get(k, {}).get('low_f')))

    # ===================
    # WEIGHTED AVERAGES ROW
    # Weights: OM(1), NWS(5), Weather.com(2), Accu(3)
    # ===================
    logger.info("[generate_pdf_report] Calculating weighted averages...")

    pdf.set_font('Helvetica', 'B', 6)
    pdf.set_text_color(0, 0, 0)
    pdf.set_fill_color(255, 220, 100)
    pdf.cell(source_col, row_h, 'Wtd. Averages', 1, 0, 'C', 1)

    weights = [1.0, 5.0, 2.0, 3.0]  # Weights: OM, NWS, Weather.com, Accu

    for i, day in enumerate(om_daily):
        k = day.get('date', '')
        # Slightly golden tint on day colors for averages row
        day_color = DAY_COLORS[i % len(DAY_COLORS)]
        avg_color = (min(255, day_color[0] + 10), min(255, day_color[1] - 10), max(0, day_color[2] - 40))
        pdf.set_fill_color(*avg_color)

        hi_vals = [
            day.get('high_f'),
            nws_daily.get(k, {}).get('high_f'),
            weathercom_daily.get(k, {}).get('high_f'),
            accu_daily.get(k, {}).get('high_f')
        ]
        lo_vals = [
            day.get('low_f'),
            nws_daily.get(k, {}).get('low_f'),
            weathercom_daily.get(k, {}).get('low_f'),
            accu_daily.get(k, {}).get('low_f')
        ]

        avg_hi = calculate_weighted_average(hi_vals, weights)
        avg_lo = calculate_weighted_average(lo_vals, weights)

        logger.debug(f"[generate_pdf_report] {k}: hi_vals={hi_vals}, avg_hi={avg_hi}")

        pdf.cell(half_col, row_h, str(avg_hi) if avg_hi else "--", 1, 0, 'C', 1)
        pdf.cell(half_col, row_h, str(avg_lo) if avg_lo else "--", 1, 0, 'C', 1)
    pdf.ln()

    # ===================
    # PRECIPITATION ROW (below Wtd. Averages)
    # Uses HRRR + Weather.com + Accu consensus
    # ===================
    pdf.set_font('Helvetica', 'B', 6)
    pdf.set_text_color(0, 0, 0)
    pdf.set_fill_color(180, 210, 255)  # Light blue for precip
    pdf.cell(source_col, row_h, 'PRECIP %', 1, 0, 'C', 1)

    for i, day in enumerate(om_daily):
        k = day.get('date', '')
        # Get consensus precip or fallback to Open-Meteo
        precip_pct = 0
        if precip_data and k in precip_data:
            precip_pct = precip_data[k].get('consensus', 0)
        else:
            precip_pct = day.get('precip_prob', 0)

        # Color based on precip probability
        if precip_pct >= 50:
            pdf.set_fill_color(100, 150, 255)  # Blue (rainy)
        elif precip_pct >= 25:
            pdf.set_fill_color(180, 210, 255)  # Light blue
        else:
            day_color = DAY_COLORS[i % len(DAY_COLORS)]
            pdf.set_fill_color(*day_color)

        pdf.set_font('Helvetica', '', 7)
        pdf.cell(day_col, row_h, f"{precip_pct}%", 1, 0, 'C', 1)
    pdf.ln()

    # Precip sources note - right-aligned below temperature matrix
    pdf.set_font('Helvetica', 'I', 5)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 3, 'PRECIP = Avg of NOAA HRRR (3km), Open-Meteo, Weather.com, AccuWeather  ', 0, 1, 'R')

    # ===================
    # SOLAR FORECAST GRID (3-Day)
    # ===================
    pdf.ln(1)
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
    # SOLAR IRRADIANCE LEGEND (directly below solar grid)
    # ===================
    pdf.ln(1)
    pdf.set_font('Helvetica', '', 6)
    pdf.set_text_color(80, 80, 80)

    # Draw legend items inline, positioned below solar grid (shifted 1" / 25.4mm to the right)
    legend_y = pdf.get_y()
    legend_x = margin + date_label_col + 25.4  # Start 1 inch to the right of hour columns
    box_w, box_h = 4, 3

    # <50 W/m² - Minimal
    pdf.set_fill_color(220, 220, 220)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 1, legend_y)
    pdf.cell(38, box_h, '<50 W/m² = Minimal', 0, 0, 'L')

    # 50-150 W/m² - Low
    legend_x += 44
    pdf.set_fill_color(200, 230, 255)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 1, legend_y)
    pdf.cell(42, box_h, '50-150 W/m² = Low-Moderate', 0, 0, 'L')

    # 150-400 W/m² - Good
    legend_x += 48
    pdf.set_fill_color(200, 255, 200)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 1, legend_y)
    pdf.cell(48, box_h, '150-400 W/m² = Good Production', 0, 0, 'L')

    # >400 W/m² - Peak
    legend_x += 54
    pdf.set_fill_color(144, 238, 144)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 1, legend_y)
    pdf.cell(48, box_h, '>400 W/m² = Peak Production', 0, 0, 'L')
    
    
    # ===================
    # SAVE PDF
    # ===================
    if output_path is None:
        pacific = ZoneInfo("America/Los_Angeles")
        timestamp = datetime.now(pacific).strftime("%Y-%m-%d_%H-%M-%S")
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
            fog_critical_hours=critical
        )
        
        if pdf_path:
            print(f"\n✅ PDF generated: {pdf_path}")
    
    asyncio.run(test())
