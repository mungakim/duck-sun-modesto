"""Regression tests for excel_report integer rounding + weighted-avg formulas."""

from pathlib import Path

from openpyxl import load_workbook

from duck_sun.excel_report import generate_excel_report


def _basic_inputs():
    om_data = {
        "daily_forecast": [
            {
                "date": "2026-05-26",
                "day_name": "Tuesday",
                # Float values are intentional - regression test for the
                # "71.0 instead of 71" rendering bug.
                "high_f": 71.4,
                "low_f": 49.6,
                "precip_prob": 20,
                "condition": "Mostly sunny",
            },
            {
                "date": "2026-05-27",
                "day_name": "Wednesday",
                "high_f": 73.0,
                "low_f": 54.0,
                "precip_prob": 7,
                "condition": "Partly sunny",
            },
        ],
        "hourly": [],
    }
    accu_data = [
        {"date": "2026-05-26", "high_f": 71, "low_f": 49, "condition": "Mostly sunny", "precip_prob": 20},
        {"date": "2026-05-27", "high_f": 73, "low_f": 54, "condition": "Partly sunny", "precip_prob": 7},
    ]
    google_data = {
        "daily": [
            {
                "date": "2026-05-26",
                "high_f": 72,
                "low_f": 55,
                "high_c": 22.1,
                "low_c": 12.8,
                "condition": "Partly sunny",
                "precip_prob": 15,
            },
        ],
        "hourly": [],
    }
    return om_data, accu_data, google_data


def test_source_row_values_are_integers_not_floats(tmp_path: Path):
    """The OPEN-METEO row used to render '71.0' / '49.0' because synthesis
    copies float high_f from upstream sources and excel_report str()'d it
    directly. All source values should be Excel integers."""
    om_data, accu_data, google_data = _basic_inputs()

    out = tmp_path / "report.xlsx"
    generate_excel_report(
        om_data=om_data,
        noaa_data=[],
        met_data=[],
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Source rows are 13-19; per-day columns start at E (col(3) with COL_OFFSET=2)
    found_numeric = 0
    for row in range(13, 20):
        for col_letter in "EFGHIJKLMNOPQRSTU":
            v = ws[f"{col_letter}{row}"].value
            if v is None:
                continue
            assert not isinstance(v, float), (
                f"Source cell {col_letter}{row} contains float {v!r}; "
                "should be int (or text marker for missing/excluded)"
            )
            if isinstance(v, int):
                found_numeric += 1
    assert found_numeric > 0, "No numeric source values found - test data wired wrong"


def test_weighted_avg_row_emits_excel_formula(tmp_path: Path):
    """Weighted average row should be a SUMPRODUCT formula that references
    the source rows, so users can edit/delete source values and the average
    recalculates live in the spreadsheet."""
    om_data, accu_data, google_data = _basic_inputs()

    out = tmp_path / "report.xlsx"
    generate_excel_report(
        om_data=om_data,
        noaa_data=[],
        met_data=[],
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Row 20 = weighted average row. Column E = day 0 high.
    formula = ws["E20"].value
    assert isinstance(formula, str) and formula.startswith("="), f"Expected formula, got {formula!r}"
    # Must reference the source range E13:E19 so cell edits propagate
    assert "E13:E19" in formula, f"Formula doesn't reference source range: {formula}"
    # Weights array literal matches the 7-source order: OM=1, NOAA=3, Met.no=3, Accu=4, Wcom=4, WU=4, Google=6
    assert "{1;3;3;4;4;4;6}" in formula, f"Weights array literal missing/wrong: {formula}"
    # Must ignore text cells ('--', '-') via ISNUMBER
    assert "ISNUMBER" in formula, f"Formula must skip text cells via ISNUMBER: {formula}"
    # Must round to int for display consistency
    assert "ROUND(" in formula, f"Formula must round to int: {formula}"
    # Must fall back to '--' when no numeric values exist (avoid #DIV/0)
    assert 'IFERROR' in formula and '"--"' in formula, f"Formula must IFERROR to '--': {formula}"


def test_weighted_avg_formula_has_both_high_and_low_columns(tmp_path: Path):
    """Both the high and low cells in each day pair should get formulas."""
    om_data, accu_data, google_data = _basic_inputs()

    out = tmp_path / "report.xlsx"
    generate_excel_report(
        om_data=om_data,
        noaa_data=[],
        met_data=[],
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Day 0: E (high) + F (low). Day 1: G (high) + H (low).
    for col_letter in ("E", "F", "G", "H"):
        v = ws[f"{col_letter}20"].value
        assert isinstance(v, str) and v.startswith("="), (
            f"Cell {col_letter}20 should be a formula; got {v!r}"
        )
        assert f"{col_letter}13:{col_letter}19" in v, (
            f"Formula at {col_letter}20 should reference its own column 13-19, got: {v}"
        )
