"""
PDF Report Generator for Duck Sun Modesto
Weights: Google(6x), Accu(4x), NOAA(3x), Met.no(3x), OM(1x)

WEIGHTED ENSEMBLE ARCHITECTURE - Google MetNet-3 Neural Model is Primary
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
    """
    Calculate daily high/low from hourly data using meteorological day (6am-6am).

    Meteorological day boundaries align with how weather services (yr.no, etc.)
    report daily highs/lows - the "high" for Tuesday is the max temp from
    Tuesday 6am to Wednesday 5:59am local time.
    """
    logger.debug(f"[calculate_daily_stats] Processing {len(hourly_data) if hourly_data else 0} hourly records (6am-6am windows)")

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

            # Meteorological day: 6am-6am window
            # Hours before 6am belong to the previous day's high/low
            if dt.hour < 6:
                met_day = dt - timedelta(days=1)
            else:
                met_day = dt

            k = met_day.strftime('%Y-%m-%d')
            if k not in daily_stats:
                daily_stats[k] = {'temps': [], 'day_name': met_day.strftime('%a')}
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

    logger.debug(f"[calculate_daily_stats] Calculated stats for {len(result)} days (6am-6am)")
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


def calculate_weighted_average_excluding_max(
    values: List[Optional[float]],
    weights: List[float]
) -> tuple[Optional[int], set[int]]:
    """
    Calculate weighted average excluding ONE highest value.

    If multiple sources have the same max, only exclude the first one found.

    Returns:
        tuple: (weighted_average, set of indices that were excluded - always 0 or 1 element)
    """
    # Find valid values with their indices
    valid_pairs = [(i, v) for i, v in enumerate(values) if v is not None]

    if not valid_pairs:
        return None, set()

    # Find the maximum value and the FIRST index that has it
    max_val = max(v for _, v in valid_pairs)
    excluded_idx = None
    for i, v in valid_pairs:
        if v == max_val:
            excluded_idx = i
            break  # Only exclude the first one

    excluded_indices = {excluded_idx} if excluded_idx is not None else set()

    # Calculate weighted average excluding ONE max value
    total_val, total_weight = 0.0, 0.0
    for i, v in valid_pairs:
        if i not in excluded_indices:
            total_val += v * weights[i]
            total_weight += weights[i]

    if total_weight > 0:
        result = round(total_val / total_weight)
        logger.debug(f"[weighted_average_excl_max] values={values}, excluded={excluded_indices}, result={result}")
        return result, excluded_indices

    # Only one value existed - return it
    return round(max_val), excluded_indices


def calculate_clear_sky_ghi(hour: int, day_of_year: int, lat: float = 37.6391) -> float:
    """
    Calculate theoretical clear-sky Global Horizontal Irradiance (GHI).

    Uses a simplified solar position model for Modesto, CA.

    Args:
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)
        lat: Latitude in degrees

    Returns:
        Clear-sky GHI in W/m² (0 if sun is below horizon)
    """
    import math

    # Solar declination angle
    declination = 23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365))

    # Hour angle (solar noon = 0)
    # Assume solar noon at 12:30 PST for Modesto
    hour_angle = 15 * (hour - 12.5)

    # Solar elevation angle
    lat_rad = math.radians(lat)
    decl_rad = math.radians(declination)
    hour_rad = math.radians(hour_angle)

    sin_elevation = (math.sin(lat_rad) * math.sin(decl_rad) +
                     math.cos(lat_rad) * math.cos(decl_rad) * math.cos(hour_rad))

    if sin_elevation <= 0:
        return 0.0  # Sun below horizon

    elevation = math.degrees(math.asin(sin_elevation))

    # Clear-sky GHI model (simplified)
    # Max GHI at solar noon in winter ~600 W/m², summer ~1000 W/m²
    # Seasonal factor based on day of year
    seasonal_factor = 0.7 + 0.3 * math.cos(math.radians((day_of_year - 172) * 360 / 365))
    max_ghi = 900 * seasonal_factor

    # GHI based on elevation angle
    ghi = max_ghi * sin_elevation

    return min(ghi, 900)  # Cap at MAX_GHI


def estimate_irradiance_from_cloud_cover(cloud_cover: int, hour: int, day_of_year: int) -> float:
    """
    Estimate solar irradiance from cloud cover percentage.

    Args:
        cloud_cover: Cloud cover percentage (0-100)
        hour: Local hour (0-23)
        day_of_year: Day of year (1-366)

    Returns:
        Estimated GHI in W/m²
    """
    # Get clear-sky GHI
    clear_sky_ghi = calculate_clear_sky_ghi(hour, day_of_year)

    if clear_sky_ghi <= 0:
        return 0.0

    # Cloud cover attenuation (70% reduction at 100% cloud cover)
    cloud_fraction = cloud_cover / 100.0
    attenuation = 1.0 - (0.7 * cloud_fraction)

    return round(clear_sky_ghi * attenuation, 1)




def get_solar_color_and_desc(risk_level: str, solar_value: float, condition: str = None) -> tuple:
    """
    Get cell color AND description based on solar conditions.

    Returns (r, g, b), description - ensures color and text are always consistent.

    Legend mapping:
    - Gray (220,220,220) = "Cloudy" (solar < 50)
    - Blue (200,230,255) = "Some Sun" (solar 50-150)
    - Light Green (200,255,200) = "Good Sun" (solar 150-400)
    - Bright Green (144,238,144) = "Full Sun" (solar > 400)
    - Yellow (255,255,180) = "Fog Possible" (MODERATE risk)
    - Orange (255,210,160) = "Heavy Clouds" (HIGH/STRATUS risk)
    - Pink (255,180,180) = "Dense Fog" (CRITICAL/FOG risk)
    - Purple/Grey (180,160,200) = "TULE FOG" (Central Valley radiation fog - DISTINCT)
    """
    risk_upper = risk_level.upper()

    # TULE FOG - Distinct purple/grey color (Central Valley specific)
    # This takes highest priority as it's a distinct weather phenomenon
    if 'TULE FOG' in risk_upper:
        return (180, 160, 200), "TULE FOG"

    # Risk-based colors take priority (weather impacts)
    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return (255, 180, 180), "Dense Fog"
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return (255, 210, 160), "Heavy Clouds"
    elif 'MODERATE' in risk_upper:
        return (255, 255, 180), "Fog Possible"

    # For LOW risk, check if we have a valid weather condition from API
    # (not "Unknown", not "Open-Meteo" - those are fallback markers)
    if condition and condition not in ('Unknown', 'Open-Meteo'):
        cond_lower = condition.lower()
        # Map weather conditions to appropriate colors based on solar impact
        if 'rain' in cond_lower or 'storm' in cond_lower or 'shower' in cond_lower:
            # Rain/storms = Heavy Clouds (orange)
            desc = "Light rain" if 'light' in cond_lower else "Rain" if 'rain' in cond_lower else "Storms"
            return (255, 210, 160), desc
        elif 'fog' in cond_lower or 'mist' in cond_lower:
            return (255, 255, 180), "Fog Possible"
        elif 'cloudy' in cond_lower:
            if 'partly' in cond_lower:
                # Partly cloudy = Some Sun (blue)
                return (200, 230, 255), "Partly cloudy"
            elif 'mostly' in cond_lower:
                # Mostly cloudy = less sun, use solar value
                if solar_value < 50:
                    return (220, 220, 220), "Mostly cloudy"
                else:
                    return (200, 230, 255), "Mostly cloudy"
            else:
                # Full cloudy
                return (220, 220, 220), "Cloudy"
        elif 'clear' in cond_lower or 'sunny' in cond_lower:
            if solar_value >= 400:
                return (144, 238, 144), "Clear, sunny"
            elif solar_value >= 150:
                return (200, 255, 200), "Clear, sunny"
            else:
                return (200, 230, 255), "Clear, sunny"

    # Fall back to solar-value based color and description (legend-consistent)
    if solar_value < 50:
        return (220, 220, 220), "Cloudy"
    elif solar_value < 150:
        return (200, 230, 255), "Some Sun"
    elif solar_value < 400:
        return (200, 255, 200), "Good Sun"
    else:
        return (144, 238, 144), "Full Sun"


# Keep old functions for backward compatibility but have them use the new one
def get_solar_color(risk_level: str, solar_value: float) -> tuple:
    """Get cell color based on solar conditions."""
    color, _ = get_solar_color_and_desc(risk_level, solar_value)
    return color


def get_descriptive_risk(risk_level: str, condition: str = None, solar_value: float = 0) -> str:
    """Get description consistent with color."""
    _, desc = get_solar_color_and_desc(risk_level, solar_value, condition)
    return desc


def get_daily_condition_display(condition: str, dewpoint_c: float = None, temp_c: float = None,
                                 visibility_low: bool = False) -> tuple:
    """
    Map Google Weather API condition to display text and color for daily descriptor.

    Detects rare Tule Fog conditions for Central Valley:
    - Temperature near or below dewpoint (high humidity)
    - Low visibility
    - Cool, calm conditions typical of radiation fog

    Returns: (display_text, background_color_rgb, text_color_rgb, is_special)
    """
    if not condition or condition == "Unknown":
        return ("--", (240, 240, 240), (100, 100, 100), False)

    cond_lower = condition.lower()

    # SPECIAL RARE CONDITIONS - Tule Fog detection for Central Valley
    # Tule fog occurs when: cold nights, high humidity (temp near dewpoint), calm winds
    is_potential_fog = False
    if dewpoint_c is not None and temp_c is not None:
        temp_dewpoint_spread = temp_c - dewpoint_c
        # If spread is < 2°C and conditions are foggy/misty, it's likely Tule fog
        if temp_dewpoint_spread < 2.0 and ('fog' in cond_lower or 'mist' in cond_lower):
            is_potential_fog = True

    if 'fog' in cond_lower or 'mist' in cond_lower:
        if is_potential_fog or visibility_low:
            # RARE TULE FOG - special formatting (red background, white text, bold)
            return ("TULE FOG", (180, 0, 0), (255, 255, 255), True)
        else:
            return ("Fog", (255, 230, 180), (80, 60, 0), False)

    # Rain conditions
    if 'thunderstorm' in cond_lower or 'storm' in cond_lower:
        return ("Storms", (100, 100, 180), (255, 255, 255), True)  # Special - rare
    elif 'heavy rain' in cond_lower:
        return ("Heavy Rain", (100, 140, 200), (255, 255, 255), False)
    elif 'rain shower' in cond_lower or 'showers' in cond_lower:
        return ("Showers", (140, 170, 220), (0, 0, 80), False)
    elif 'light rain' in cond_lower:
        return ("Light Rain", (180, 200, 230), (0, 0, 80), False)
    elif 'drizzle' in cond_lower:
        return ("Drizzle", (180, 200, 230), (0, 0, 80), False)
    elif 'rain' in cond_lower:
        return ("Rain", (120, 160, 210), (255, 255, 255), False)

    # Snow (rare for Central Valley - special)
    if 'snow' in cond_lower or 'sleet' in cond_lower or 'ice' in cond_lower:
        return ("SNOW", (200, 220, 255), (0, 0, 120), True)  # Special - rare

    # Cloud conditions
    if 'overcast' in cond_lower:
        return ("Overcast", (200, 200, 200), (40, 40, 40), False)
    elif 'cloudy' in cond_lower:
        if 'partly' in cond_lower:
            return ("Partly Cloudy", (230, 245, 255), (40, 60, 80), False)
        elif 'mostly' in cond_lower:
            return ("Mostly Cloudy", (210, 220, 230), (40, 50, 60), False)
        else:
            return ("Cloudy", (200, 210, 220), (40, 50, 60), False)

    # Clear/Sunny conditions
    if 'clear' in cond_lower or 'sunny' in cond_lower:
        return ("Sunny", (255, 250, 200), (120, 100, 0), False)
    elif 'fair' in cond_lower:
        return ("Fair", (250, 250, 220), (100, 90, 0), False)

    # Haze/Smoke (can be special during fire season)
    if 'haze' in cond_lower or 'smoke' in cond_lower:
        return ("SMOKE/HAZE", (255, 200, 150), (120, 60, 0), True)  # Special

    # Wind
    if 'wind' in cond_lower:
        return ("Windy", (230, 240, 255), (60, 80, 120), False)

    # Default - use the condition as-is, truncated
    display = condition[:12] if len(condition) > 12 else condition
    return (display, (245, 245, 245), (60, 60, 60), False)


def generate_pdf_report(
    om_data: Dict,
    noaa_data: Optional[List],
    met_data: Optional[List],
    accu_data: Optional[List],
    google_data: Optional[Dict] = None,
    df_analyzed: pd.DataFrame = None,
    fog_critical_hours: int = 0,
    output_path: Optional[Path] = None,
    mid_data: Optional[Dict] = None,
    hrrr_data: Optional[Dict] = None,
    precip_data: Optional[Dict] = None,
    degraded_sources: Optional[List[str]] = None,
    noaa_daily_periods: Optional[Dict] = None,
    report_timestamp: Optional[datetime] = None
) -> Optional[Path]:
    """
    Generate PDF report with 5-source temperature grid and weighted consensus.

    Args:
        om_data: Open-Meteo forecast data
        noaa_data: NOAA hourly data (fallback if period data unavailable)
        met_data: Met.no hourly data (ECMWF European model)
        accu_data: AccuWeather daily data (5-day forecast)
        google_data: Google Weather API data (MetNet-3 neural model) - HIGHEST WEIGHT
        df_analyzed: Analyzed dataframe with solar/fog data
        fog_critical_hours: Number of critical fog hours
        output_path: Output path for PDF
        mid_data: MID.org 48-hour summary data
        hrrr_data: HRRR model data (48-hour, 3km resolution)
        precip_data: Aggregated precipitation probabilities by date
        degraded_sources: List of providers using cached/stale data
        noaa_daily_periods: PRIORITY - NOAA Period-based daily stats (matches website)
        report_timestamp: Optional timestamp to use (ensures filename and content match)
    """

    if not HAS_FPDF:
        logger.error("[generate_pdf_report] fpdf2 not installed")
        return None

    logger.info("[generate_pdf_report] Starting PDF generation...")
    logger.info(f"[generate_pdf_report] AccuWeather data: {len(accu_data) if accu_data else 0} days")
    
    # Process data sources
    om_daily = om_data.get('daily_forecast', [])[:8]
    met_daily = calculate_daily_stats_from_hourly(met_data) if met_data else {}

    # PRIORITY: Use NOAA Period Data if available (matches website)
    if noaa_daily_periods:
        logger.info("[generate_pdf_report] Using NOAA Period Data (Website Match)")
        noaa_daily = noaa_daily_periods
    else:
        # Fallback to calculating from hourly grid (Legacy/Risk of mismatch)
        logger.info("[generate_pdf_report] Falling back to NOAA hourly aggregation")
        noaa_daily = calculate_daily_stats_from_hourly(noaa_data) if noaa_data else {}

    logger.info(f"[generate_pdf_report] Met.no processed: {len(met_daily)} days")

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

    # Process Google Weather data (MetNet-3 neural model - HIGHEST WEIGHT)
    google_daily = {}
    if google_data:
        logger.info(f"[generate_pdf_report] Google data keys: {list(google_data.keys()) if isinstance(google_data, dict) else 'NOT A DICT'}")
        daily_list = google_data.get('daily', [])
        logger.info(f"[generate_pdf_report] Google daily_list length: {len(daily_list)}")
        if daily_list:
            logger.info(f"[generate_pdf_report] Google first daily entry: {daily_list[0]}")
        for d in daily_list:
            if 'high_f' in d and 'low_f' in d:
                google_daily[d['date']] = {
                    'high_f': int(d['high_f']),
                    'low_f': int(d['low_f'])
                }
            elif 'high_c' in d and 'low_c' in d:
                google_daily[d['date']] = {
                    'high_f': round(d['high_c'] * 1.8 + 32),
                    'low_f': round(d['low_c'] * 1.8 + 32)
                }
        logger.info(f"[generate_pdf_report] Google Weather processed: {len(google_daily)} days (MetNet-3)")
    else:
        logger.warning(f"[generate_pdf_report] google_data is None or empty!")

    pdf = DuckSunPDF()
    pdf.add_page()
    margin = 8
    usable_width = 279 - (2 * margin)

    # Use passed-in timestamp to ensure filename and content match
    if report_timestamp:
        report_time = report_timestamp
    else:
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
    weight_col = 6  # Weight score column (blank header)
    source_col = 22
    day_col = (usable_width - weight_col - source_col) / 8
    half_col, row_h = day_col / 2, 6

    # Source weights for display (calibrated Dec 2025 + Google MetNet-3)
    SOURCE_WEIGHT_DISPLAY = {
        'OPEN-METEO': '1.0',
        'NOAA (GOV)': '3.0',
        'MET.NO (EU)': '3.0',
        'ACCU (COM)': '4.0',
        'GOOGLE (AI)': '10.0',  # MetNet-3 neural model - HIGHEST
    }

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

    # ===================
    # BUILD MERGED CONDITIONS MAP
    # Priority: Google (best) → AccuWeather → Open-Meteo (always 8 days)
    # ===================
    daily_conditions = {}

    # Step 1: Open-Meteo as base (always has 8 days from WMO weather codes)
    for day_record in om_daily:
        date_key = day_record.get('date', '')
        condition = day_record.get('condition', 'Unknown')
        if condition and condition != 'Unknown':
            daily_conditions[date_key] = {'condition': condition, 'source': 'Open-Meteo'}

    # Step 2: AccuWeather overwrites (better quality, 5 days)
    if accu_data:
        for day_record in accu_data:
            date_key = day_record.get('date', '')
            condition = day_record.get('condition', '')
            if condition and condition != 'Unknown':
                daily_conditions[date_key] = {'condition': condition, 'source': 'AccuWeather'}

    # Step 3: Google overwrites (best quality, 4-5 days)
    if google_data:
        for day_record in google_data.get('daily', []):
            date_key = day_record.get('date', '')
            condition = day_record.get('condition', '')
            if condition and condition != 'Unknown':
                daily_conditions[date_key] = {'condition': condition, 'source': 'Google'}

    logger.info(f"[generate_pdf_report] Merged conditions: {len(daily_conditions)} days")
    for date_key, info in list(daily_conditions.items())[:3]:
        logger.debug(f"[generate_pdf_report]   {date_key}: {info['condition']} ({info['source']})")

    # ===================
    # CONDITION DESCRIPTORS ROW (Above Day Names)
    # Shows overall daily weather condition for each day
    # ===================
    pdf.set_font('Helvetica', 'B', 6)
    pdf.set_text_color(80, 80, 80)
    pdf.set_fill_color(250, 250, 250)
    pdf.cell(weight_col + source_col, row_h - 1, '', 1, 0, 'C', 1)  # Merged blank cell (no label)

    for i, day in enumerate(om_daily):
        date_key = day.get('date', '')
        condition_info = daily_conditions.get(date_key, {'condition': 'Unknown', 'source': None})
        condition = condition_info['condition']

        # Get display text and colors
        display_text, bg_color, text_color, is_special = get_daily_condition_display(condition)

        # Set colors
        pdf.set_fill_color(*bg_color)
        pdf.set_text_color(*text_color)

        # Bold for special conditions (rare weather)
        if is_special:
            pdf.set_font('Helvetica', 'B', 6)
        else:
            pdf.set_font('Helvetica', '', 6)

        pdf.cell(day_col, row_h - 1, display_text, 1, 0, 'C', 1)

    pdf.ln()
    pdf.set_text_color(0, 0, 0)  # Reset text color

    # Header Row (Day Names) - Color coded by day
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_fill_color(0, 60, 120)
    pdf.cell(weight_col, row_h, '', 1, 0, 'C', 1)  # Blank weight header
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
    pdf.cell(weight_col, row_h-1, '', 1, 0, 'C', 1)  # Blank weight cell
    pdf.cell(source_col, row_h-1, 'DATE', 1, 0, 'C', 1)
    for i, day in enumerate(om_daily):
        date_str = day.get('date', '')[5:]  # MM-DD
        base_color = DAY_COLORS[i % len(DAY_COLORS)]
        dark_color = (max(0, base_color[0] - 70), max(0, base_color[1] - 50), max(0, base_color[2] - 30))
        pdf.set_fill_color(*dark_color)
        pdf.cell(day_col, row_h-1, date_str, 1, 0, 'C', 1)
    pdf.ln()

    # Pre-calculate which high value is excluded (highest) for each day
    # excluded_highs[day_index] = set with ONE source index (0=OM, 1=NOAA, 2=Met.no, 3=Accu, 4=Google)
    excluded_highs = {}
    for i, day in enumerate(om_daily):
        k = day.get('date', '')
        hi_vals = [
            day.get('high_f'),
            noaa_daily.get(k, {}).get('high_f'),
            met_daily.get(k, {}).get('high_f'),
            accu_daily.get(k, {}).get('high_f'),
            google_daily.get(k, {}).get('high_f')
        ]
        # Find max value and the FIRST index that has it (only exclude one)
        valid_highs = [(idx, v) for idx, v in enumerate(hi_vals) if v is not None]
        if valid_highs:
            max_high = max(v for _, v in valid_highs)
            for idx, v in valid_highs:
                if v == max_high:
                    excluded_highs[i] = {idx}  # Only exclude the first one
                    break
        else:
            excluded_highs[i] = set()

    def draw_row_colored(label: str, getter, source_idx: int):
        """Draw a single row with weight + source name + color-coded Hi/Lo cells.
        Highlights excluded (max) high values in red."""
        pdf.set_text_color(0, 0, 0)

        # Weight column (light gray)
        pdf.set_fill_color(230, 230, 230)
        pdf.set_font('Helvetica', '', 6)
        weight_val = SOURCE_WEIGHT_DISPLAY.get(label, '')
        pdf.cell(weight_col, row_h, weight_val, 1, 0, 'C', 1)

        # Source label (neutral gray)
        pdf.set_fill_color(245, 245, 245)
        pdf.set_font('Helvetica', 'B', 6)
        pdf.cell(source_col, row_h, label, 1, 0, 'C', 1)

        # Temperature cells - COLOR CODED BY DAY
        pdf.set_font('Helvetica', '', 8)  # 15% larger than original 7pt
        for i, d in enumerate(om_daily):
            v1, v2 = getter(d, d.get('date', ''))

            # Check if this high value is excluded (max)
            is_excluded_high = source_idx in excluded_highs.get(i, set())

            # High cell - red background if excluded, else day color
            if is_excluded_high and v1 is not None:
                pdf.set_fill_color(255, 180, 180)  # Light red for excluded
            else:
                day_color = DAY_COLORS[i % len(DAY_COLORS)]
                pdf.set_fill_color(*day_color)
            pdf.cell(half_col, row_h, str(v1) if v1 else "--", 1, 0, 'C', 1)

            # Low cell - always day color
            day_color = DAY_COLORS[i % len(DAY_COLORS)]
            pdf.set_fill_color(*day_color)
            pdf.cell(half_col, row_h, str(v2) if v2 else "--", 1, 0, 'C', 1)
        pdf.ln()

    # Draw source rows with day-colored columns (pass source index for exclusion tracking)
    draw_row_colored('OPEN-METEO',
             lambda d, k: (d.get('high_f'), d.get('low_f')), 0)

    draw_row_colored('NOAA (GOV)',
             lambda d, k: (noaa_daily.get(k, {}).get('high_f'), noaa_daily.get(k, {}).get('low_f')), 1)

    draw_row_colored('MET.NO (EU)',
             lambda d, k: (met_daily.get(k, {}).get('high_f'), met_daily.get(k, {}).get('low_f')), 2)

    draw_row_colored('ACCU (COM)',
             lambda d, k: (accu_daily.get(k, {}).get('high_f'), accu_daily.get(k, {}).get('low_f')), 3)

    draw_row_colored('GOOGLE (AI)',
             lambda d, k: (google_daily.get(k, {}).get('high_f'), google_daily.get(k, {}).get('low_f')), 4)

    # ===================
    # WEIGHTED AVERAGES ROW
    # Weights: OM(1), NOAA(3), Met.no(3), Accu(4), Google(6) - Dec 2025
    # EXCLUDES HIGHEST HIGH VALUE(S) FROM CALCULATION
    # ===================
    logger.info("[generate_pdf_report] Calculating weighted averages (excluding max highs)...")

    pdf.set_font('Helvetica', 'B', 6)
    pdf.set_text_color(0, 0, 0)
    pdf.set_fill_color(255, 220, 100)
    pdf.cell(weight_col, row_h, '', 1, 0, 'C', 1)  # Blank weight cell for averages row
    pdf.cell(source_col, row_h, 'Wtd. Averages', 1, 0, 'C', 1)

    # Weights: OM, NOAA, Met.no, Accu, Google (calibrated Dec 2025)
    weights = [1.0, 3.0, 3.0, 4.0, 6.0]

    pdf.set_font('Helvetica', 'B', 8)  # 15% larger for weighted average values
    for i, day in enumerate(om_daily):
        k = day.get('date', '')
        # Slightly golden tint on day colors for averages row
        day_color = DAY_COLORS[i % len(DAY_COLORS)]
        avg_color = (min(255, day_color[0] + 10), min(255, day_color[1] - 10), max(0, day_color[2] - 40))
        pdf.set_fill_color(*avg_color)

        hi_vals = [
            day.get('high_f'),
            noaa_daily.get(k, {}).get('high_f'),
            met_daily.get(k, {}).get('high_f'),
            accu_daily.get(k, {}).get('high_f'),
            google_daily.get(k, {}).get('high_f')
        ]
        lo_vals = [
            day.get('low_f'),
            noaa_daily.get(k, {}).get('low_f'),
            met_daily.get(k, {}).get('low_f'),
            accu_daily.get(k, {}).get('low_f'),
            google_daily.get(k, {}).get('low_f')
        ]

        # Calculate high average EXCLUDING the max value(s)
        avg_hi, excluded = calculate_weighted_average_excluding_max(hi_vals, weights)
        # Low average uses all values (no exclusion)
        avg_lo = calculate_weighted_average(lo_vals, weights)

        logger.debug(f"[generate_pdf_report] {k}: hi_vals={hi_vals}, excluded={excluded}, avg_hi={avg_hi}")

        pdf.cell(half_col, row_h, str(avg_hi) if avg_hi else "--", 1, 0, 'C', 1)
        pdf.cell(half_col, row_h, str(avg_lo) if avg_lo else "--", 1, 0, 'C', 1)
    pdf.ln()

    # ===================
    # PRECIPITATION ROW (below Wtd. Averages)
    # Uses HRRR + Open-Meteo + Accu consensus
    # ===================
    pdf.set_font('Helvetica', 'B', 6)
    pdf.set_text_color(0, 0, 0)
    pdf.set_fill_color(180, 210, 255)  # Light blue for precip
    pdf.cell(weight_col, row_h, '', 1, 0, 'C', 1)  # Blank weight cell for precip row
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

        pdf.set_font('Helvetica', '', 8)  # 15% larger for consistency
        pdf.cell(day_col, row_h, f"{precip_pct}%", 1, 0, 'C', 1)
    pdf.ln()

    # Precip sources note - right-aligned below temperature matrix
    pdf.set_font('Helvetica', 'I', 5)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 3, 'PRECIP = Google (0-72hr) > AccuWeather (72hr+) > Open-Meteo  ', 0, 1, 'R')

    # ===================
    # SOLAR FORECAST GRID (4-Day: Today + 3 Days)
    # Uses Google Weather API cloud cover → estimated irradiance
    # ===================
    pdf.ln(1)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(0, 60, 120)
    pdf.cell(0, 5, 'SOLAR FORECAST (GOOGLE AI WEATHER API) - W/m² Irradiance', 0, 1, 'L')

    logger.info("[generate_pdf_report] Drawing solar forecast grid (Google Weather)...")

    tz = ZoneInfo("America/Los_Angeles")
    # 4 days: today + next 3 days
    forecast_dates = [(datetime.now(tz) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(0, 4)]

    # Build duck curve data from Google Weather hourly cloud cover
    duck_data = {d: [] for d in forecast_dates}

    # Get Google hourly data
    google_hourly = google_data.get('hourly', []) if google_data else []

    for hour_record in google_hourly:
        try:
            time_str = hour_record.get('time', '')
            if not time_str:
                continue

            # Parse UTC time and convert to local
            if 'Z' in time_str:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00')).astimezone(tz)
            else:
                dt = datetime.fromisoformat(time_str).astimezone(tz)

            row_date = dt.strftime('%Y-%m-%d')
            row_hour = dt.hour

            # Duck curve hours: 9 AM to 4 PM (HE09-HE16)
            if row_date in forecast_dates and 9 <= row_hour <= 16:
                cloud_cover = hour_record.get('cloud_cover', 50)
                day_of_year = dt.timetuple().tm_yday
                condition = hour_record.get('condition', 'Unknown')

                # Estimate irradiance from cloud cover
                irradiance = estimate_irradiance_from_cloud_cover(cloud_cover, row_hour, day_of_year)

                # Determine risk level from condition
                condition_lower = condition.lower()
                if 'rain' in condition_lower or 'storm' in condition_lower:
                    risk = 'HIGH'
                elif cloud_cover >= 90:
                    risk = 'MODERATE'
                elif cloud_cover >= 70:
                    risk = 'LOW-MOD'
                else:
                    risk = 'LOW'

                duck_data[row_date].append({
                    'hour': row_hour,
                    'solar': irradiance,
                    'risk': risk,
                    'condition': condition
                })
        except Exception as e:
            logger.debug(f"[generate_pdf_report] Error processing Google hour: {e}")
            continue

    # Fallback to df_analyzed if no Google data available
    if not any(duck_data.values()) and df_analyzed is not None:
        logger.info("[generate_pdf_report] Falling back to ensemble data for solar forecast")
        for _, row in df_analyzed.iterrows():
            try:
                row_date = row['time'].strftime('%Y-%m-%d')
                row_hour = row['time'].hour
                if row_date in forecast_dates and 9 <= row_hour <= 16:
                    duck_data[row_date].append({
                        'hour': row_hour,
                        'solar': row.get('solar_adjusted', 0),
                        'risk': row.get('risk_level', 'LOW'),
                        'condition': None  # Will use solar-based description
                    })
            except Exception as e:
                logger.debug(f"[generate_pdf_report] Error processing row: {e}")
                continue

    # Fill gaps for TODAY using Open-Meteo if Google doesn't have past hours
    # (Google API only returns data from "now" forward, so morning hours may be missing)
    today = datetime.now(tz).strftime('%Y-%m-%d')
    if today in forecast_dates and df_analyzed is not None:
        existing_hours = {h['hour'] for h in duck_data.get(today, [])}
        missing_duck_hours = [h for h in range(9, 17) if h not in existing_hours]

        if missing_duck_hours:
            logger.info(f"[generate_pdf_report] Filling {len(missing_duck_hours)} missing hours for today from Open-Meteo")
            for _, row in df_analyzed.iterrows():
                try:
                    row_date = row['time'].strftime('%Y-%m-%d')
                    row_hour = row['time'].hour
                    if row_date == today and row_hour in missing_duck_hours:
                        # Estimate cloud-adjusted irradiance from Open-Meteo solar data
                        solar_val = row.get('solar_adjusted', 0)
                        if solar_val == 0:
                            solar_val = row.get('solar_raw', 0)

                        duck_data[today].append({
                            'hour': row_hour,
                            'solar': solar_val,
                            'risk': row.get('risk_level', 'LOW'),
                            'condition': None  # Will use solar-based description
                        })
                except Exception as e:
                    logger.debug(f"[generate_pdf_report] Error filling gap: {e}")
                    continue

            # Sort today's hours after adding
            duck_data[today].sort(key=lambda x: x['hour'])

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
    for d in forecast_dates:
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
            h_data = hours_dict.get(9+i, {'solar': 0, 'risk': 'LOW', 'condition': 'Unknown'})
            # Boost solar value by 15% for display (calibration adjustment)
            solar_display = h_data['solar'] * 1.15
            condition = h_data.get('condition', 'Unknown')

            # Get BOTH color and description from same function (ensures consistency)
            (r, g, b), risk_desc = get_solar_color_and_desc(
                h_data['risk'], solar_display, condition
            )
            pdf.set_fill_color(r, g, b)

            # Solar value (boosted 15%)
            pdf.set_xy(x_start + i * hour_col, y_start)
            pdf.cell(hour_col, solar_row_h, f"{solar_display:.0f}", 1, 0, 'C', 1)

            # Condition label - consistent with color
            pdf.set_xy(x_start + i * hour_col, y_start + solar_row_h)
            pdf.set_font('Helvetica', 'I', 6)
            pdf.cell(hour_col, solar_row_h, risk_desc, 1, 0, 'C', 1)
            pdf.set_font('Helvetica', '', 6)
        
        # Move to next row
        pdf.set_xy(row_x_start, row_y_start + solar_row_h * 2)

    # ===================
    # SOLAR IRRADIANCE LEGEND (single line, compact)
    # ===================
    pdf.ln(1)
    pdf.set_font('Helvetica', '', 5)
    pdf.set_text_color(80, 80, 80)

    # Draw all legend items on one line
    legend_y = pdf.get_y()
    legend_x = margin + 2
    box_w, box_h = 3, 2.5

    # Cloudy/No Sun - Gray
    pdf.set_fill_color(220, 220, 220)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(18, box_h, 'Cloudy', 0, 0, 'L')

    # Some Sun - Blue
    legend_x += 22
    pdf.set_fill_color(200, 230, 255)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(18, box_h, 'Some Sun', 0, 0, 'L')

    # Good Sun - Light Green
    legend_x += 22
    pdf.set_fill_color(200, 255, 200)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(18, box_h, 'Good Sun', 0, 0, 'L')

    # Full Sun - Bright Green
    legend_x += 22
    pdf.set_fill_color(144, 238, 144)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(18, box_h, 'Full Sun', 0, 0, 'L')

    # Fog Possible - Yellow
    legend_x += 24
    pdf.set_fill_color(255, 255, 180)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(20, box_h, 'Fog Possible', 0, 0, 'L')

    # Heavy Clouds - Orange
    legend_x += 24
    pdf.set_fill_color(255, 210, 160)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(22, box_h, 'Heavy Clouds', 0, 0, 'L')

    # Dense Fog - Pink
    legend_x += 26
    pdf.set_fill_color(255, 180, 180)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(18, box_h, 'Dense Fog', 0, 0, 'L')

    # Tule Fog - Purple/Grey (Central Valley specific)
    legend_x += 22
    pdf.set_fill_color(180, 160, 200)
    pdf.rect(legend_x, legend_y, box_w, box_h, 'F')
    pdf.set_xy(legend_x + box_w + 0.5, legend_y)
    pdf.cell(16, box_h, 'Tule Fog', 0, 0, 'L')

    # Units note
    legend_x += 18
    pdf.set_xy(legend_x, legend_y)
    pdf.set_font('Helvetica', 'I', 4)
    pdf.cell(30, box_h, '(values = W/m²)', 0, 0, 'L')
    
    
    # ===================
    # SAVE PDF
    # ===================
    if output_path is None:
        pacific = ZoneInfo("America/Los_Angeles")
        now = datetime.now(pacific)
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        output_path = Path("reports") / now.strftime("%Y-%m") / now.strftime("%Y-%m-%d") / f"daily_forecast_{timestamp}.pdf"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    
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
    from duck_sun.providers.noaa import NOAAProvider
    from duck_sun.providers.met_no import MetNoProvider
    from duck_sun.providers.accuweather import AccuWeatherProvider
    from duck_sun.uncanniness import UncannyEngine
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    async def test():
        print("=== Testing PDF Report Generator (Hybrid Architecture) ===\n")

        om_data = await fetch_open_meteo(days=8)

        noaa = NOAAProvider()
        noaa_data = await noaa.fetch_async()

        met = MetNoProvider()
        met_data = await met.fetch_async()

        accu = AccuWeatherProvider()
        accu_data = await accu.fetch_forecast()

        engine = UncannyEngine()
        df = engine.normalize_temps(om_data, noaa_data, met_data)
        df_analyzed = engine.analyze_duck_curve(df)

        critical = len(df_analyzed[df_analyzed['risk_level'].str.contains('CRITICAL', na=False)])

        pdf_path = generate_pdf_report(
            om_data=om_data,
            noaa_data=noaa_data,
            met_data=met_data,
            accu_data=accu_data,
            df_analyzed=df_analyzed,
            fog_critical_hours=critical
        )

        if pdf_path:
            print(f"\n✅ PDF generated: {pdf_path}")

    asyncio.run(test())
