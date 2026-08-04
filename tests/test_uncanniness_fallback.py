import pandas as pd

from duck_sun.uncanniness import UncannyEngine


def test_normalize_temps_falls_back_to_google_timeline_when_open_meteo_missing():
    engine = UncannyEngine()

    om_data = {"daily_summary": []}
    google_data = {
        "hourly": [
            {"time": "2026-05-26T08:00:00-07:00"},
            {"time": "2026-05-26T09:00:00-07:00"},
            {"time": "2026-05-26T10:00:00-07:00"},
        ],
        "daily": [
            {"date": "2026-05-26", "high_c": 25.0, "low_c": 12.0}
        ],
    }
    noaa_data = [{"time": "2026-05-26T15:00:00Z", "temp_c": 20.0}]

    df = engine.normalize_temps(
        om_data=om_data,
        noaa_data=noaa_data,
        google_data=google_data,
    )

    assert len(df) >= 3
    assert "temp_om" in df.columns
    assert df["temp_om"].isna().all()
    assert pd.api.types.is_datetime64_any_dtype(df["time"])



def test_fallback_output_contains_required_physics_columns_for_duck_curve():
    engine = UncannyEngine()

    om_data = {"daily_summary": []}
    google_data = {
        "hourly": [{"time": "2026-05-26T08:00:00-07:00"}],
        "daily": [{"date": "2026-05-26", "high_c": 25.0, "low_c": 12.0}],
    }

    df = engine.normalize_temps(
        om_data=om_data,
        noaa_data=None,
        google_data=google_data,
    )

    required_cols = ["dew_point_c", "cloud_cover", "wind_speed_kph", "radiation", "dni", "precip_prob", "precip_mm"]
    for col in required_cols:
        assert col in df.columns

    analyzed = engine.analyze_duck_curve(df, google_hourly=google_data["hourly"])
    assert "risk_level" in analyzed.columns
