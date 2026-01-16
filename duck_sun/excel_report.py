"""
Excel Report Generator for Duck Sun Modesto
Generates Excel (.xlsx) reports matching the PDF format - CENTERED LAYOUT

Weights: Google(6x), Accu(4x), Weather.com(4x), WUnderground(4x), NOAA(3x), Met.no(3x), OM(1x)
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

try:
    from openpyxl import Workbook
    from openpyxl.styles import (
        Font, PatternFill, Border, Side, Alignment,
        NamedStyle
    )
    from openpyxl.utils import get_column_letter
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    Workbook = None

logger = logging.getLogger(__name__)

# Column offset for centering (start content at column E)
COL_OFFSET = 4


def col(n):
    """Get column letter with offset for centering."""
    return get_column_letter(n + COL_OFFSET)


def calculate_daily_stats_from_hourly(hourly_data: List[Dict], timezone: str = "America/Los_Angeles") -> Dict:
    """Calculate daily high/low from hourly data using meteorological day (6am-6am)."""
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

    return result


def calculate_weighted_average(values: List[Optional[float]], weights: List[float]) -> Optional[int]:
    """Calculate weighted average from values with weights."""
    total_val, total_weight = 0.0, 0.0

    for val, weight in zip(values, weights):
        if val is not None:
            total_val += val * weight
            total_weight += weight

    if total_weight > 0:
        return round(total_val / total_weight)
    return None


def calculate_weighted_average_excluding_om_max(
    values: List[Optional[float]],
    weights: List[float]
) -> tuple[Optional[int], set[int]]:
    """Calculate weighted average, excluding Open-Meteo (index 0) only if it has the max value."""
    valid_pairs = [(i, v) for i, v in enumerate(values) if v is not None]

    if not valid_pairs:
        return None, set()

    max_val = max(v for _, v in valid_pairs)

    om_val = values[0]
    excluded_indices = set()
    if om_val is not None and om_val == max_val:
        excluded_indices = {0}

    total_val, total_weight = 0.0, 0.0
    for i, v in valid_pairs:
        if i not in excluded_indices:
            total_val += v * weights[i]
            total_weight += weights[i]

    if total_weight > 0:
        return round(total_val / total_weight), excluded_indices

    if om_val is not None:
        return round(om_val), excluded_indices
    return None, excluded_indices


def calculate_clear_sky_ghi(hour: int, day_of_year: int, lat: float = 37.6391) -> float:
    """Calculate theoretical clear-sky Global Horizontal Irradiance (GHI)."""
    import math

    declination = 23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365))
    hour_angle = 15 * (hour - 12.5)

    lat_rad = math.radians(lat)
    decl_rad = math.radians(declination)
    hour_rad = math.radians(hour_angle)

    sin_elevation = (math.sin(lat_rad) * math.sin(decl_rad) +
                     math.cos(lat_rad) * math.cos(decl_rad) * math.cos(hour_rad))

    if sin_elevation <= 0:
        return 0.0

    seasonal_factor = 0.7 + 0.3 * math.cos(math.radians((day_of_year - 172) * 360 / 365))
    max_ghi = 900 * seasonal_factor

    ghi = max_ghi * sin_elevation

    return min(ghi, 900)


def estimate_irradiance_from_cloud_cover(cloud_cover: int, hour: int, day_of_year: int) -> float:
    """Estimate solar irradiance from cloud cover percentage."""
    clear_sky_ghi = calculate_clear_sky_ghi(hour, day_of_year)

    if clear_sky_ghi <= 0:
        return 0.0

    cloud_fraction = cloud_cover / 100.0
    attenuation = 1.0 - (0.7 * cloud_fraction)

    return round(clear_sky_ghi * attenuation, 1)


def get_solar_color_and_desc(risk_level: str, solar_value: float, condition: str = None) -> tuple:
    """Get cell color AND description based on solar conditions."""
    risk_upper = risk_level.upper()

    if 'TULE FOG' in risk_upper:
        return "B4A0C8", "TULE FOG"

    if 'CRITICAL' in risk_upper or 'ACTIVE FOG' in risk_upper:
        return "FFB4B4", "Dense Fog"
    elif 'HIGH' in risk_upper or 'STRATUS' in risk_upper:
        return "FFD2A0", "Heavy Clouds"
    elif 'MODERATE' in risk_upper:
        return "FFFFB4", "Fog Possible"

    if condition and condition not in ('Unknown', 'Open-Meteo'):
        cond_lower = condition.lower()
        if 'rain' in cond_lower or 'storm' in cond_lower or 'shower' in cond_lower:
            desc = "Light rain" if 'light' in cond_lower else "Rain" if 'rain' in cond_lower else "Storms"
            return "FFD2A0", desc
        elif 'fog' in cond_lower or 'mist' in cond_lower:
            return "FFFFB4", "Fog Possible"
        elif 'cloudy' in cond_lower:
            if 'partly' in cond_lower:
                return "C8E6FF", "Partly cloudy"
            elif 'mostly' in cond_lower:
                if solar_value < 50:
                    return "DCDCDC", "Mostly cloudy"
                else:
                    return "C8E6FF", "Mostly cloudy"
            else:
                return "DCDCDC", "Cloudy"
        elif 'clear' in cond_lower or 'sunny' in cond_lower:
            if solar_value >= 400:
                return "90EE90", "Full Sun"
            elif solar_value >= 150:
                return "C8FFC8", "Good Sun"
            else:
                return "C8E6FF", "Clear"

    if solar_value < 50:
        return "DCDCDC", "Cloudy"
    elif solar_value < 150:
        return "C8E6FF", "Some Sun"
    elif solar_value < 400:
        return "C8FFC8", "Good Sun"
    else:
        return "90EE90", "Full Sun"


def get_daily_condition_display(condition: str) -> tuple:
    """Map condition to display text and color for daily descriptor."""
    if not condition or condition == "Unknown":
        return ("--", "F0F0F0")

    cond_lower = condition.lower()

    if 'fog' in cond_lower or 'mist' in cond_lower:
        return ("Fog", "FFE6B4")
    if 'thunderstorm' in cond_lower or 'storm' in cond_lower:
        return ("Storms", "6464B4")
    elif 'rain' in cond_lower:
        return ("Rain", "78A0D2")
    if 'snow' in cond_lower or 'sleet' in cond_lower:
        return ("Snow", "C8DCFF")
    if 'overcast' in cond_lower:
        return ("Overcast", "C8C8C8")
    elif 'cloudy' in cond_lower:
        if 'partly' in cond_lower:
            return ("Partly Cloudy", "E6F5FF")
        elif 'mostly' in cond_lower:
            return ("Mostly Cloudy", "D2DCE6")
        else:
            return ("Cloudy", "C8D2DC")
    if 'clear' in cond_lower or 'sunny' in cond_lower:
        return ("Sunny", "FFFAC8")
    if 'haze' in cond_lower or 'smoke' in cond_lower:
        return ("Haze", "FFC896")

    display = condition[:12] if len(condition) > 12 else condition
    return (display, "F5F5F5")


def generate_excel_report(
    om_data: Dict,
    noaa_data: Optional[List],
    met_data: Optional[List],
    accu_data: Optional[List],
    google_data: Optional[Dict] = None,
    weather_com_data: Optional[List] = None,
    wunderground_data: Optional[List] = None,
    df_analyzed: Any = None,
    output_path: Optional[Path] = None,
    mid_data: Optional[Dict] = None,
    precip_data: Optional[Dict] = None,
    noaa_daily_periods: Optional[Dict] = None,
    report_timestamp: Optional[datetime] = None
) -> Optional[Path]:
    """Generate Excel report with 7-source temperature grid and weighted consensus."""
    if not HAS_OPENPYXL:
        logger.error("[generate_excel_report] openpyxl not installed")
        return None

    logger.info("[generate_excel_report] Starting Excel generation (CENTERED)...")

    # Process data sources
    om_daily = om_data.get('daily_forecast', [])[:8]
    met_daily = calculate_daily_stats_from_hourly(met_data) if met_data else {}
    noaa_daily = noaa_daily_periods if noaa_daily_periods else (
        calculate_daily_stats_from_hourly(noaa_data) if noaa_data else {}
    )

    # Process AccuWeather data
    accu_daily = {}
    if accu_data:
        for d in accu_data:
            if 'high_f' in d and 'low_f' in d:
                accu_daily[d['date']] = {'high_f': int(d['high_f']), 'low_f': int(d['low_f'])}
            elif 'high_c' in d and 'low_c' in d:
                accu_daily[d['date']] = {
                    'high_f': round(d['high_c'] * 1.8 + 32),
                    'low_f': round(d['low_c'] * 1.8 + 32)
                }

    # Process Google Weather data
    google_daily = {}
    if google_data:
        for d in google_data.get('daily', []):
            if 'high_f' in d and 'low_f' in d:
                google_daily[d['date']] = {'high_f': int(d['high_f']), 'low_f': int(d['low_f'])}
            elif 'high_c' in d and 'low_c' in d:
                google_daily[d['date']] = {
                    'high_f': round(d['high_c'] * 1.8 + 32),
                    'low_f': round(d['low_c'] * 1.8 + 32)
                }

    # Process Weather.com data
    weather_com_daily = {}
    if weather_com_data:
        for d in weather_com_data:
            if 'high_f' in d and 'low_f' in d:
                weather_com_daily[d['date']] = {'high_f': int(d['high_f']), 'low_f': int(d['low_f'])}

    # Process Weather Underground data
    wunderground_daily = {}
    if wunderground_data:
        for d in wunderground_data:
            if 'high_f' in d and 'low_f' in d:
                wunderground_daily[d['date']] = {'high_f': int(d['high_f']), 'low_f': int(d['low_f'])}

    # Create workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Daily Forecast"

    # Define styles
    thin = Side(style='thin')
    med = Side(style='medium')
    thin_border = Border(left=thin, right=thin, top=thin, bottom=thin)
    thick_border = Border(left=med, right=med, top=med, bottom=med)
    center = Alignment(horizontal='center', vertical='center')

    # Timestamp
    report_time = report_timestamp or datetime.now(ZoneInfo("America/Los_Angeles"))
    timestamp_str = report_time.strftime("%A, %B %d, %Y %H:%M:%S")

    # Set column widths - A-D are left margin
    for c in ['A', 'B', 'C', 'D']:
        ws.column_dimensions[c].width = 2
    ws.column_dimensions[col(1)].width = 5    # Weight
    ws.column_dimensions[col(2)].width = 13   # Source
    for i in range(3, 20):
        ws.column_dimensions[col(i)].width = 5.5

    # ==================== TITLE ====================
    ws.merge_cells(f'{col(1)}2:{col(18)}2')
    ws[f'{col(1)}2'].value = "MODESTO, CA - DAILY WEATHER FORECAST"
    ws[f'{col(1)}2'].font = Font(name='Arial', size=14, bold=True, color='003C78')
    ws[f'{col(1)}2'].alignment = center

    ws.merge_cells(f'{col(1)}3:{col(18)}3')
    ws[f'{col(1)}3'].value = timestamp_str
    ws[f'{col(1)}3'].font = Font(name='Arial', size=10, bold=True)
    ws[f'{col(1)}3'].alignment = center

    # ==================== PGE CITYGATE (top left) ====================
    ws[f'{col(1)}5'] = "PGE CITYGATE:"
    ws[f'{col(1)}5'].font = Font(name='Arial', size=8, bold=True)
    ws[f'{col(2)}5'].border = thick_border
    ws[f'{col(2)}5'].alignment = center

    # ==================== MID GAS BURN (below PGE) - 6 cells (2x3) ====================
    ws[f'{col(1)}7'] = "MID GAS BURN:"
    ws[f'{col(1)}7'].font = Font(name='Arial', size=8, bold=True)

    for row in range(8, 11):
        for c in range(1, 3):
            cell = ws[f'{col(c)}{row}']
            cell.border = thick_border
            cell.alignment = center

    # ==================== MID WEATHER 48-HOUR SUMMARY (top right) ====================
    ws.merge_cells(f'{col(12)}5:{col(18)}5')
    ws[f'{col(12)}5'].value = "MID WEATHER 48-HOUR SUMMARY"
    ws[f'{col(12)}5'].font = Font(name='Arial', size=9, bold=True, color='003C78')
    ws[f'{col(12)}5'].fill = PatternFill(start_color="F0F8FF", end_color="F0F8FF", fill_type="solid")
    ws[f'{col(12)}5'].alignment = center
    ws[f'{col(12)}5'].border = thick_border

    # Headers
    for c, lbl in [(14, 'High'), (15, 'Low'), (16, 'Rain')]:
        ws[f'{col(c)}6'].value = lbl
        ws[f'{col(c)}6'].font = Font(name='Arial', size=7, bold=True)
        ws[f'{col(c)}6'].alignment = center
        ws[f'{col(c)}6'].border = thin_border

    if mid_data:
        today_d = mid_data.get('today', {})
        yest_d = mid_data.get('yesterday', {})

        # TODAY
        ws[f'{col(12)}7'] = "TODAY"
        ws[f'{col(12)}7'].font = Font(name='Arial', size=7, bold=True)
        ws[f'{col(12)}7'].border = thick_border

        ws[f'{col(14)}7'] = f"{today_d.get('high', '--')}F"
        ws[f'{col(14)}7'].fill = PatternFill(start_color="FFC8B4", end_color="FFC8B4", fill_type="solid")
        ws[f'{col(14)}7'].alignment = center
        ws[f'{col(14)}7'].border = thick_border

        ws[f'{col(15)}7'] = f"{today_d.get('low', '--')}F"
        ws[f'{col(15)}7'].fill = PatternFill(start_color="B4D2FF", end_color="B4D2FF", fill_type="solid")
        ws[f'{col(15)}7'].alignment = center
        ws[f'{col(15)}7'].border = thick_border

        ws[f'{col(16)}7'] = f"{today_d.get('rain', '0.00')}\""
        ws[f'{col(16)}7'].alignment = center
        ws[f'{col(16)}7'].border = thick_border

        # YEST
        ws[f'{col(12)}8'] = "YEST"
        ws[f'{col(12)}8'].font = Font(name='Arial', size=7, bold=True)
        ws[f'{col(12)}8'].border = thick_border

        ws[f'{col(14)}8'] = f"{yest_d.get('high', '--')}F"
        ws[f'{col(14)}8'].fill = PatternFill(start_color="FFC8B4", end_color="FFC8B4", fill_type="solid")
        ws[f'{col(14)}8'].alignment = center
        ws[f'{col(14)}8'].border = thick_border

        ws[f'{col(15)}8'] = f"{yest_d.get('low', '--')}F"
        ws[f'{col(15)}8'].fill = PatternFill(start_color="B4D2FF", end_color="B4D2FF", fill_type="solid")
        ws[f'{col(15)}8'].alignment = center
        ws[f'{col(15)}8'].border = thick_border

        ws[f'{col(16)}8'] = f"{yest_d.get('rain', '0.00')}\""
        ws[f'{col(16)}8'].alignment = center
        ws[f'{col(16)}8'].border = thick_border

        # Records
        if 'record_high_temp' in mid_data:
            ws.merge_cells(f'{col(12)}9:{col(18)}9')
            rec = ws[f'{col(12)}9']
            rec.value = f"Records: Hi {mid_data.get('record_high_temp')}F({mid_data.get('record_high_year')}) Lo {mid_data.get('record_low_temp')}F({mid_data.get('record_low_year')})"
            rec.font = Font(name='Arial', size=6, italic=True)
            rec.alignment = center

    # ==================== TEMPERATURE GRID ====================
    DAY_COLORS = ["FFF0F0", "F0FFF0", "F0F8FF", "FFFFF0", "FFF5EE", "F5FFFA", "F8F8FF", "FFFAF0"]

    # Build conditions map
    daily_conditions = {}
    for d in om_daily:
        if d.get('condition') and d.get('condition') != 'Unknown':
            daily_conditions[d['date']] = d['condition']
    if accu_data:
        for d in accu_data:
            if d.get('condition') and d.get('condition') != 'Unknown':
                daily_conditions[d['date']] = d['condition']
    if google_data:
        for d in google_data.get('daily', []):
            if d.get('condition') and d.get('condition') != 'Unknown':
                daily_conditions[d['date']] = d['condition']

    # Row 12: Conditions
    row = 12
    ws[f'{col(1)}{row}'].border = thin_border
    ws[f'{col(2)}{row}'].border = thin_border
    for i, day in enumerate(om_daily):
        cond = daily_conditions.get(day['date'], 'Unknown')
        txt, bg = get_daily_condition_display(cond)
        col_hi, col_lo = col(3 + i*2), col(4 + i*2)
        ws.merge_cells(f'{col_hi}{row}:{col_lo}{row}')
        c = ws[f'{col_hi}{row}']
        c.value = txt
        c.fill = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
        c.font = Font(name='Arial', size=7)
        c.alignment = center
        c.border = thin_border

    # Row 13: Day headers
    row = 13
    ws[f'{col(1)}{row}'].fill = PatternFill(start_color="003C78", end_color="003C78", fill_type="solid")
    ws[f'{col(1)}{row}'].border = thin_border
    ws[f'{col(2)}{row}'] = "SOURCE"
    ws[f'{col(2)}{row}'].fill = PatternFill(start_color="003C78", end_color="003C78", fill_type="solid")
    ws[f'{col(2)}{row}'].font = Font(name='Arial', size=8, bold=True, color='FFFFFF')
    ws[f'{col(2)}{row}'].alignment = center
    ws[f'{col(2)}{row}'].border = thin_border

    for i, day in enumerate(om_daily):
        lbl = "TODAY" if i == 0 else day.get('day_name', '')[:3].upper()
        col_hi, col_lo = col(3 + i*2), col(4 + i*2)
        ws.merge_cells(f'{col_hi}{row}:{col_lo}{row}')
        c = ws[f'{col_hi}{row}']
        c.value = lbl
        c.fill = PatternFill(start_color="003C78", end_color="003C78", fill_type="solid")
        c.font = Font(name='Arial', size=8, bold=True, color='FFFFFF')
        c.alignment = center
        c.border = thin_border

    # Row 14: Dates
    row = 14
    ws[f'{col(1)}{row}'].fill = PatternFill(start_color="466EA0", end_color="466EA0", fill_type="solid")
    ws[f'{col(1)}{row}'].border = thin_border
    ws[f'{col(2)}{row}'] = "DATE"
    ws[f'{col(2)}{row}'].fill = PatternFill(start_color="466EA0", end_color="466EA0", fill_type="solid")
    ws[f'{col(2)}{row}'].font = Font(name='Arial', size=7, color='FFFFFF')
    ws[f'{col(2)}{row}'].alignment = center
    ws[f'{col(2)}{row}'].border = thin_border

    for i, day in enumerate(om_daily):
        col_hi, col_lo = col(3 + i*2), col(4 + i*2)
        ws.merge_cells(f'{col_hi}{row}:{col_lo}{row}')
        c = ws[f'{col_hi}{row}']
        c.value = day['date'][5:]
        c.fill = PatternFill(start_color="466EA0", end_color="466EA0", fill_type="solid")
        c.font = Font(name='Arial', size=7, color='FFFFFF')
        c.alignment = center
        c.border = thin_border

    # Pre-calculate excluded highs
    weights = [1.0, 3.0, 3.0, 4.0, 4.0, 4.0, 6.0]
    excluded_highs = {}
    for i, day in enumerate(om_daily):
        k = day['date']
        hi_vals = [
            day.get('high_f'), noaa_daily.get(k, {}).get('high_f'),
            met_daily.get(k, {}).get('high_f'), accu_daily.get(k, {}).get('high_f'),
            weather_com_daily.get(k, {}).get('high_f'), wunderground_daily.get(k, {}).get('high_f'),
            google_daily.get(k, {}).get('high_f')
        ]
        valid = [(idx, v) for idx, v in enumerate(hi_vals) if v is not None]
        if valid:
            mx = max(v for _, v in valid)
            excluded_highs[i] = {0} if hi_vals[0] is not None and hi_vals[0] == mx else set()
        else:
            excluded_highs[i] = set()

    # Source rows
    sources = [
        ('OPEN-METEO', '1.0', lambda d, k: (d.get('high_f'), d.get('low_f')), 0),
        ('NOAA (GOV)', '3.0', lambda d, k: (noaa_daily.get(k, {}).get('high_f'), noaa_daily.get(k, {}).get('low_f')), 1),
        ('MET.NO (EU)', '3.0', lambda d, k: (met_daily.get(k, {}).get('high_f'), met_daily.get(k, {}).get('low_f')), 2),
        ('ACCU (COM)', '4.0', lambda d, k: (accu_daily.get(k, {}).get('high_f'), accu_daily.get(k, {}).get('low_f')), 3),
        ('WEATHER.COM', '4.0', lambda d, k: (weather_com_daily.get(k, {}).get('high_f'), weather_com_daily.get(k, {}).get('low_f')), 4),
        ('WUNDERGRND', '4.0', lambda d, k: (wunderground_daily.get(k, {}).get('high_f'), wunderground_daily.get(k, {}).get('low_f')), 5),
        ('GOOGLE (AI)', '6.0', lambda d, k: (google_daily.get(k, {}).get('high_f'), google_daily.get(k, {}).get('low_f')), 6),
    ]

    for src_idx, (label, wt, getter, sidx) in enumerate(sources):
        row = 15 + src_idx
        ws[f'{col(1)}{row}'] = wt
        ws[f'{col(1)}{row}'].fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
        ws[f'{col(1)}{row}'].font = Font(name='Arial', size=7)
        ws[f'{col(1)}{row}'].alignment = center
        ws[f'{col(1)}{row}'].border = thin_border

        ws[f'{col(2)}{row}'] = label
        ws[f'{col(2)}{row}'].fill = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
        ws[f'{col(2)}{row}'].font = Font(name='Arial', size=7, bold=True)
        ws[f'{col(2)}{row}'].alignment = center
        ws[f'{col(2)}{row}'].border = thin_border

        for i, day in enumerate(om_daily):
            k = day['date']
            hi, lo = getter(day, k)
            excl = sidx in excluded_highs.get(i, set())
            clr = DAY_COLORS[i % len(DAY_COLORS)]

            ch = ws[f'{col(3 + i*2)}{row}']
            ch.value = "-" if excl and hi else (str(hi) if hi else "--")
            ch.fill = PatternFill(start_color=clr, end_color=clr, fill_type="solid")
            ch.font = Font(name='Arial', size=9)
            ch.alignment = center
            ch.border = thin_border

            cl = ws[f'{col(4 + i*2)}{row}']
            cl.value = str(lo) if lo else "--"
            cl.fill = PatternFill(start_color=clr, end_color=clr, fill_type="solid")
            cl.font = Font(name='Arial', size=9)
            cl.alignment = center
            cl.border = thin_border

    # Weighted Averages row
    row = 22
    ws[f'{col(1)}{row}'].fill = PatternFill(start_color="FFDC64", end_color="FFDC64", fill_type="solid")
    ws[f'{col(1)}{row}'].border = thin_border
    ws[f'{col(2)}{row}'] = "Wtd. Averages"
    ws[f'{col(2)}{row}'].fill = PatternFill(start_color="FFDC64", end_color="FFDC64", fill_type="solid")
    ws[f'{col(2)}{row}'].font = Font(name='Arial', size=7, bold=True)
    ws[f'{col(2)}{row}'].alignment = center
    ws[f'{col(2)}{row}'].border = thin_border

    for i, day in enumerate(om_daily):
        k = day['date']
        hi_vals = [day.get('high_f'), noaa_daily.get(k, {}).get('high_f'), met_daily.get(k, {}).get('high_f'),
                   accu_daily.get(k, {}).get('high_f'), weather_com_daily.get(k, {}).get('high_f'),
                   wunderground_daily.get(k, {}).get('high_f'), google_daily.get(k, {}).get('high_f')]
        lo_vals = [day.get('low_f'), noaa_daily.get(k, {}).get('low_f'), met_daily.get(k, {}).get('low_f'),
                   accu_daily.get(k, {}).get('low_f'), weather_com_daily.get(k, {}).get('low_f'),
                   wunderground_daily.get(k, {}).get('low_f'), google_daily.get(k, {}).get('low_f')]

        avg_hi, _ = calculate_weighted_average_excluding_om_max(hi_vals, weights)
        avg_lo = calculate_weighted_average(lo_vals, weights)

        ch = ws[f'{col(3 + i*2)}{row}']
        ch.value = str(avg_hi) if avg_hi else "--"
        ch.fill = PatternFill(start_color="FFDC64", end_color="FFDC64", fill_type="solid")
        ch.font = Font(name='Arial', size=9, bold=True)
        ch.alignment = center
        ch.border = thin_border

        cl = ws[f'{col(4 + i*2)}{row}']
        cl.value = str(avg_lo) if avg_lo else "--"
        cl.fill = PatternFill(start_color="FFDC64", end_color="FFDC64", fill_type="solid")
        cl.font = Font(name='Arial', size=9, bold=True)
        cl.alignment = center
        cl.border = thin_border

    # PRECIP % row
    row = 23
    ws[f'{col(1)}{row}'].fill = PatternFill(start_color="B4D2FF", end_color="B4D2FF", fill_type="solid")
    ws[f'{col(1)}{row}'].border = thin_border
    ws[f'{col(2)}{row}'] = "PRECIP %"
    ws[f'{col(2)}{row}'].fill = PatternFill(start_color="B4D2FF", end_color="B4D2FF", fill_type="solid")
    ws[f'{col(2)}{row}'].font = Font(name='Arial', size=7, bold=True)
    ws[f'{col(2)}{row}'].alignment = center
    ws[f'{col(2)}{row}'].border = thin_border

    for i, day in enumerate(om_daily):
        k = day['date']
        pct = precip_data.get(k, {}).get('consensus', 0) if precip_data else day.get('precip_prob', 0)
        clr = "6496FF" if pct >= 50 else ("B4D2FF" if pct >= 25 else DAY_COLORS[i % len(DAY_COLORS)])

        col_hi, col_lo = col(3 + i*2), col(4 + i*2)
        ws.merge_cells(f'{col_hi}{row}:{col_lo}{row}')
        c = ws[f'{col_hi}{row}']
        c.value = f"{pct}%"
        c.fill = PatternFill(start_color=clr, end_color=clr, fill_type="solid")
        c.font = Font(name='Arial', size=9)
        c.alignment = center
        c.border = thin_border
        ws[f'{col_lo}{row}'].border = thin_border

    # Precip note
    ws.merge_cells(f'{col(12)}24:{col(18)}24')
    ws[f'{col(12)}24'].value = "PRECIP = Google (0-72hr) > AccuWeather (72hr+) > Open-Meteo"
    ws[f'{col(12)}24'].font = Font(name='Arial', size=6, italic=True, color='505050')
    ws[f'{col(12)}24'].alignment = Alignment(horizontal='right', vertical='center')

    # ==================== SOLAR FORECAST (wider layout) ====================
    row = 26
    ws.merge_cells(f'{col(1)}{row}:{col(10)}{row}')
    ws[f'{col(1)}{row}'] = "SOLAR FORECAST (GOOGLE AI WEATHER API) - W/m² Irradiance"
    ws[f'{col(1)}{row}'].font = Font(name='Arial', size=10, bold=True, color='003C78')

    tz = ZoneInfo("America/Los_Angeles")
    forecast_dates = [(datetime.now(tz) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(4)]

    # Build duck curve data
    duck_data = {d: [] for d in forecast_dates}
    google_hourly = google_data.get('hourly', []) if google_data else []

    for hr in google_hourly:
        try:
            ts = hr.get('time', '')
            if not ts:
                continue
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(tz) if 'Z' in ts else datetime.fromisoformat(ts).astimezone(tz)
            rd, rh = dt.strftime('%Y-%m-%d'), dt.hour
            if rd in forecast_dates and 9 <= rh <= 16:
                cc = hr.get('cloud_cover', 50)
                irr = estimate_irradiance_from_cloud_cover(cc, rh, dt.timetuple().tm_yday)
                cond = hr.get('condition', 'Unknown')
                risk = 'HIGH' if 'rain' in cond.lower() else ('MODERATE' if cc >= 90 else 'LOW')
                duck_data[rd].append({'hour': rh, 'solar': irr, 'risk': risk, 'condition': cond})
        except:
            continue

    # Solar header - wider columns
    row = 27
    headers = ['DATE', '9AM', '10AM', '11AM', '12PM', '1PM', '2PM', '3PM', '4PM']
    for i, h in enumerate(headers):
        c = ws[f'{col(1 + i)}{row}']
        c.value = h
        c.fill = PatternFill(start_color="003C78", end_color="003C78", fill_type="solid")
        c.font = Font(name='Arial', size=8, bold=True, color='FFFFFF')
        c.alignment = center
        c.border = thin_border

    # Solar data rows - single row per day (cleaner)
    for di, d in enumerate(forecast_dates):
        row = 28 + di
        date_obj = datetime.strptime(d, '%Y-%m-%d')

        # Date cell
        dc = ws[f'{col(1)}{row}']
        dc.value = f"{d[5:]} {date_obj.strftime('%a')}"
        dc.fill = PatternFill(start_color="F0F0F0", end_color="F0F0F0", fill_type="solid")
        dc.font = Font(name='Arial', size=8, bold=True)
        dc.alignment = center
        dc.border = thin_border

        hours_dict = {h['hour']: h for h in duck_data.get(d, [])}
        for hi in range(8):
            hd = hours_dict.get(9 + hi, {'solar': 0, 'risk': 'LOW', 'condition': 'Unknown'})
            sv = int(hd['solar'] * 1.15)
            clr, _ = get_solar_color_and_desc(hd['risk'], sv, hd.get('condition'))

            c = ws[f'{col(2 + hi)}{row}']
            c.value = sv
            c.fill = PatternFill(start_color=clr, end_color=clr, fill_type="solid")
            c.font = Font(name='Arial', size=9)
            c.alignment = center
            c.border = thin_border

    # Legend row
    row = 33
    legend = [("Cloudy", "DCDCDC"), ("Some Sun", "C8E6FF"), ("Good Sun", "C8FFC8"), ("Full Sun", "90EE90"),
              ("Fog", "FFFFB4"), ("Heavy Cld", "FFD2A0"), ("Dense Fog", "FFB4B4"), ("Tule Fog", "B4A0C8")]
    for i, (lbl, clr) in enumerate(legend):
        c = ws[f'{col(1 + i)}{row}']
        c.value = lbl
        c.fill = PatternFill(start_color=clr, end_color=clr, fill_type="solid")
        c.font = Font(name='Arial', size=6)
        c.alignment = center
        c.border = thin_border

    ws[f'{col(9)}{row}'] = "(W/m²)"
    ws[f'{col(9)}{row}'].font = Font(name='Arial', size=6, italic=True, color='505050')

    # Save
    if output_path is None:
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        output_path = Path("reports") / now.strftime("%Y-%m") / now.strftime("%Y-%m-%d") / f"daily_forecast_{now.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        wb.save(str(output_path))
        logger.info(f"[generate_excel_report] Excel saved to: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"[generate_excel_report] Failed to save Excel: {e}")
        return None


if __name__ == "__main__":
    import asyncio
    from duck_sun.providers.open_meteo import fetch_open_meteo
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    async def test():
        om_data = await fetch_open_meteo(days=8)
        generate_excel_report(om_data=om_data, noaa_data=None, met_data=None, accu_data=None)

    asyncio.run(test())
