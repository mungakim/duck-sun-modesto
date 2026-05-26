"""Regression tests for solar descriptor logic in excel_report."""

from duck_sun.excel_report import get_solar_color_and_desc


def test_high_solar_overrides_storm_mention_in_condition():
    """May 26 X: drive incident: Solar grid showed 'Storms' for 818 W/m²
    hours with Google's condition text mentioning a storm. When the call
    site happens to leave risk at LOW (no risk='HIGH' override), the
    text-based descriptor branch would render 'Storms'. Above ~250 W/m²
    the sun isn't actually obscured, so we trust the irradiance number
    over the wording."""
    # risk='LOW' is what makes the function fall through to the condition-text
    # branch — that's the path the gate guards.
    _, desc = get_solar_color_and_desc(
        risk_level="LOW",
        solar_value=818,
        condition="Isolated thunderstorms",
    )
    assert desc != "Storms", (
        f"818 W/m² should not be labeled 'Storms' even with storm in condition; got {desc!r}"
    )


def test_low_solar_with_storm_condition_still_renders_storms():
    """When solar IS low AND condition mentions storm AND risk doesn't pre-empt
    (e.g. condition was 'storm' but call-site risk assignment was LOW), the
    storm label is legitimate."""
    _, desc = get_solar_color_and_desc(
        risk_level="LOW",
        solar_value=80,  # well below the 250 W/m² gate
        condition="Thunderstorms",
    )
    assert desc == "Storms", f"Low-solar storm condition should render 'Storms', got {desc!r}"


def test_low_solar_with_rain_condition_renders_rain():
    _, desc = get_solar_color_and_desc(
        risk_level="LOW",
        solar_value=50,
        condition="Light rain",
    )
    assert desc == "Lt rain"


def test_high_solar_with_sunny_condition_unaffected():
    """Sanity check: the storm gate doesn't accidentally suppress normal labels."""
    _, desc = get_solar_color_and_desc(
        risk_level="LOW",
        solar_value=700,
        condition="Sunny",
    )
    assert desc == "Sunny"


def test_high_risk_still_returns_overcast_regardless_of_solar():
    """When UncannyEngine flags the hour as HIGH risk (persistent stratus,
    confirmed storm, etc.), the risk-based descriptor wins over solar."""
    _, desc = get_solar_color_and_desc(
        risk_level="HIGH (PERSISTENT STRATUS)",
        solar_value=800,
        condition="Sunny",
    )
    assert desc == "Overcast"
