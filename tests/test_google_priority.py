"""Regression tests for the Jul 2026 "Google is never demoted" calibration.

Google (MetNet-3) is the preferred source everywhere on the report EXCEPT the
word-descriptor row, which must read like weather.com.
"""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from openpyxl import load_workbook

from duck_sun.ensemble import WeightedEnsembleEngine
from duck_sun.excel_report import generate_excel_report
from duck_sun.solar_physics import (
    calculate_hybrid_solar,
    calculate_solar_from_cloud_cover,
    calculate_theoretical_max_ghi,
)

NOON = 12
SUMMER_DAY = 212  # ~Jul 31


# ---------------------------------------------------------------- ensemble --

def test_google_keeps_full_weight_when_wildly_off_peer_median():
    """The old veto demoted Google 8.0 -> 2.0 at >10F deviation. It must not."""
    engine = WeightedEnsembleEngine()

    # Google 20C vs peers all clustered near 32C => ~21F deviation
    result = engine.compute_consensus(
        {
            "Google": 20.0,
            "AccuWeather": 32.0,
            "Weather.com": 32.0,
            "WUnderground": 31.8,
            "NOAA": 31.5,
            "Open-Meteo": 32.0,
        },
        unit="C",
    )

    # source_contributions holds normalized shares; effective_weights holds the raw weight
    assert result.diagnostics["effective_weights"]["Google"] == 8.0, (
        f"Google was demoted to {result.diagnostics['effective_weights']['Google']}; "
        "the veto must be gone"
    )
    assert result.diagnostics["google_veto_triggered"] is False
    assert result.diagnostics["google_veto_severity"] is None
    # Deviation is still measured for observability
    assert result.diagnostics["google_peer_delta_f"] > 10.0


def test_google_full_weight_pulls_consensus_toward_it():
    """With weight 6 intact, a low Google reading must move the consensus."""
    engine = WeightedEnsembleEngine()
    sources = {
        "Google": 20.0,
        "AccuWeather": 32.0,
        "NOAA": 31.5,
        "Open-Meteo": 32.0,
    }
    with_google = engine.compute_consensus(dict(sources), unit="C").consensus_value

    no_google = dict(sources)
    no_google.pop("Google")
    without_google = engine.compute_consensus(no_google, unit="C").consensus_value

    assert with_google < without_google, (
        "Google at weight 6 should drag the consensus down; "
        f"got {with_google} vs {without_google} without it"
    )


def test_google_weight_is_highest_in_the_table():
    weights = WeightedEnsembleEngine.SOURCE_WEIGHTS
    assert weights["Google"] == 8.0, "Google was raised 6 -> 8 in Jul 2026"
    assert weights["Google"] == max(weights.values())
    # Google alone should outvote any single peer by 2x
    assert weights["Google"] >= 2 * weights["AccuWeather"]


# ------------------------------------------------------------ solar source --

def test_solar_prefers_google_cloud_over_open_meteo_radiation():
    """Open-Meteo radiation must not influence the result when Google has data."""
    clear = calculate_hybrid_solar(om_radiation=50.0, google_cloud=0, hour=NOON, day_of_year=SUMMER_DAY)
    expected = calculate_solar_from_cloud_cover(0, NOON, SUMMER_DAY)
    assert abs(clear - expected) < 0.01, "Google-clear hour should ignore the low OM value"

    # Same Google reading, wildly different OM value -> identical answer
    same = calculate_hybrid_solar(om_radiation=900.0, google_cloud=0, hour=NOON, day_of_year=SUMMER_DAY)
    assert abs(clear - same) < 0.01, "OM radiation must not change a Google-driven hour"


def test_solar_falls_back_to_open_meteo_only_when_google_is_none():
    fallback = calculate_hybrid_solar(
        om_radiation=333.0, google_cloud=None, hour=NOON, day_of_year=SUMMER_DAY
    )
    assert fallback == 333.0, "With no Google cloud, OM radiation should pass through"


def test_solar_is_zero_when_neither_source_has_data():
    assert calculate_hybrid_solar(0, None, NOON, SUMMER_DAY) == 0.0


def test_solar_is_zero_at_night_regardless_of_source():
    assert calculate_hybrid_solar(500.0, 0, 2, SUMMER_DAY) == 0.0


def test_cloud_cover_monotonically_reduces_irradiance():
    values = [calculate_solar_from_cloud_cover(c, NOON, SUMMER_DAY) for c in (0, 25, 50, 75, 100)]
    assert values == sorted(values, reverse=True), f"Not monotonic: {values}"
    assert values[0] == calculate_theoretical_max_ghi(NOON, SUMMER_DAY)


def test_excel_and_physics_share_one_irradiance_model():
    """The grid and the JSON outlook must not drift apart."""
    from duck_sun.excel_report import estimate_irradiance_from_cloud_cover

    for cloud in (0, 30, 60, 100):
        grid = estimate_irradiance_from_cloud_cover(cloud, NOON, SUMMER_DAY)
        physics = calculate_solar_from_cloud_cover(cloud, NOON, SUMMER_DAY)
        assert abs(grid - round(physics, 1)) < 0.01, f"Divergence at cloud={cloud}%"


# ------------------------------------------------------------ excel report --

def _report(tmp_path: Path, **overrides):
    tz = ZoneInfo("America/Los_Angeles")
    now = datetime.now(tz)
    dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]

    kwargs = dict(
        om_data={
            "daily_forecast": [
                {"date": d, "day_name": "X", "high_f": 95, "low_f": 62,
                 "precip_prob": 5, "condition": "Clear sky"}
                for d in dates
            ],
            "hourly": [],
        },
        noaa_data=[],
        accu_data=[{"date": d, "high_f": 96, "low_f": 62, "condition": "Mostly sunny"}
                   for d in dates[:5]],
        google_data={
            "daily": [{"date": d, "high_f": 97, "low_f": 63, "condition": "Sunny"} for d in dates],
            "hourly": [
                {"time": f"{d}T{h:02d}:00:00-07:00", "temp_c": 30.0, "cloud_cover": 10,
                 "precip_prob": 0, "condition": "Sunny", "is_daytime": True}
                for d in dates for h in range(9, 17)
            ],
        },
        weather_com_data=[{"date": d, "high_f": 96, "low_f": 62,
                           "condition": "Sunshine and clouds", "precip_prob": 3}
                          for d in dates],
        output_path=tmp_path / "r.xlsx",
        report_timestamp=now,
    )
    kwargs.update(overrides)
    generate_excel_report(**kwargs)
    return load_workbook(kwargs["output_path"]).active


def test_descriptor_row_comes_from_weather_com_not_google(tmp_path: Path):
    ws = _report(tmp_path)
    # Row 10 holds the descriptors; day columns start at E.
    # "Sunshine and clouds" is a known wordy TWC phrase -> abbreviated.
    for col_letter in ("E", "G", "I", "K", "M", "O", "Q", "S"):
        assert ws[f"{col_letter}10"].value == "Sun/Clouds", (
            f"{col_letter}10 = {ws[f'{col_letter}10'].value!r}; expected Weather.com wording"
        )


def test_long_conditions_never_get_cut_mid_word():
    """Weather.com's wxPhraseLong is verbose; hard truncation gave 'Sunshine And C'."""
    from duck_sun.excel_report import fit_condition_text

    assert fit_condition_text("Sunshine And Patchy Clouds") == "Sun/Clouds"
    assert fit_condition_text("Scattered Thunderstorms") == "Sctd Storms"
    assert fit_condition_text("Mostly Sunny") == "Mostly Sunny"   # already short

    # Unknown long phrase: drop whole trailing words, never split one
    fitted = fit_condition_text("Freezing Drizzle Late Tonight")
    assert len(fitted) <= 14
    assert not fitted.endswith(" ")
    for word in fitted.split():
        assert word in "Freezing Drizzle Late Tonight".split(), (
            f"{word!r} is a fragment, not a whole word"
        )


def test_descriptors_fall_back_when_weather_com_is_missing(tmp_path: Path):
    """Weather.com is blocked on some networks - descriptors must still render."""
    ws = _report(tmp_path, weather_com_data=None)
    assert ws["E10"].value == "Mostly Sunny"          # AccuWeather covers days 0-4
    assert ws["Q10"].value == "Sunny"                 # Google covers the outer days


def test_solar_grid_spans_seven_days(tmp_path: Path):
    """7 days, one shorter than the 8-day temperature grid, to stay compact."""
    ws = _report(tmp_path)
    # Header sits under the side-reference band (rows 22-25) plus one spacer
    assert ws["D28"].value == "DATE"
    # 7 days x 2 rows starting at 29 => last descriptor row is 42
    for date_idx in range(7):
        row = 29 + date_idx * 2
        assert ws[f"D{row}"].value, f"Solar day row {row} is empty"
        assert isinstance(ws[f"E{row}"].value, int), f"No irradiance value at E{row}"

    # Nothing beyond day 7, and the legend sits one blank row below the block
    assert ws["D43"].value in (None, ""), "An 8th solar day leaked back in"
    assert ws["E44"].value == "Tule Fog"


def test_solar_grid_is_entirely_google_when_google_hourly_is_complete(tmp_path: Path):
    """No Open-Meteo fill should be needed with the 240-hour pull."""
    ws = _report(tmp_path, df_analyzed=None)
    for date_idx in range(7):
        row = 29 + date_idx * 2
        for col_letter in "EFGHIJKL":
            assert ws[f"{col_letter}{row}"].value, (
                f"{col_letter}{row} empty - Google should have covered every cell"
            )
