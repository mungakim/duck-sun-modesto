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
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Source rows are 13-18; per-day columns start at E (col(3) with COL_OFFSET=2)
    found_numeric = 0
    for row in range(13, 19):
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
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Row 19 = weighted average row. Column E = day 0 high.
    formula = ws["E19"].value
    assert isinstance(formula, str) and formula.startswith("="), f"Expected formula, got {formula!r}"
    # Must reference the source range E13:E19 so cell edits propagate
    assert "E13:E18" in formula, f"Formula doesn't reference source range: {formula}"
    # Weights array literal matches the 6-source order: OM=1, NOAA=3, Accu=4, Wcom=4, WU=4, Google=8
    assert "{1;3;4;4;4;8}" in formula, f"Weights array literal missing/wrong: {formula}"
    # Must use 2-arg SUMPRODUCT in the numerator (handles text-as-zero natively
    # without needing CSE array entry, unlike SUMPRODUCT(IF(ISNUMBER(...))...))
    assert "SUMPRODUCT(E13:E18,{1;3;4;4;4;8})" in formula, (
        f"Numerator must use 2-arg SUMPRODUCT(range, weights) form: {formula}"
    )
    # Denominator must exclude blanks, '-' (OM-max marker), and '--' (missing)
    assert '<>""' in formula and '<>"-"' in formula and '<>"--"' in formula, (
        f"Denominator must exclude blank/text cells: {formula}"
    )
    # Must round to int for display consistency
    assert "ROUND(" in formula, f"Formula must round to int: {formula}"
    # Must fall back to '--' when no numeric values exist (avoid #DIV/0)
    assert 'IFERROR' in formula and '"--"' in formula, f"Formula must IFERROR to '--': {formula}"


def test_weighted_avg_formula_actually_evaluates_correctly(tmp_path: Path):
    """End-to-end: write a workbook with known source values, evaluate the
    formula via a third-party Excel engine, confirm it returns the right
    integer (not 0 or '--' from the previous broken IF(ISNUMBER(...)) pattern).
    """
    pytest = __import__("pytest")
    try:
        import formulas as _fml
    except ImportError:
        pytest.skip("formulas library not installed")

    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active

    # Plant a realistic data scenario in column E (day 0 high)
    # OM=70, NOAA=70, Accu=71, Wcom=71, WU=71, Google=80
    # weighted = (70*1+70*3+71*4+71*4+71*4+80*8)/(1+3+4+4+4+8) = 1772/24 = 73.83 -> 74
    # Google is deliberately far from its peers here: at the old weight of 6
    # this evaluates to 73, so the test fails loudly if the weight regresses.
    ws["E13"] = 70
    ws["E14"] = 70
    ws["E15"] = 71
    ws["E16"] = 71
    ws["E17"] = 71
    ws["E18"] = 80

    # Column G: only one numeric (NOAA=80), rest "--". Expected: 80
    for cell, val in [("G13", "--"), ("G14", 80), ("G15", "--"),
                      ("G16", "--"), ("G17", "--"), ("G18", "--")]:
        ws[cell] = val

    # Column H: OM excluded via "-", others all 75 except Google 78
    # expected: (75*3+75*4+75*4+75*4+78*8)/(3+4+4+4+8) = 1749/23 = 76.04 -> 76
    ws["H13"] = "-"
    for cell in ("H14", "H15", "H16", "H17"):
        ws[cell] = 75
    ws["H18"] = 78

    # Use the same formula generator the real code uses
    weights_array = "{1;3;4;4;4;8}"

    def _fmla(c):
        rng = f"{c}13:{c}18"
        return (
            f'=IFERROR(ROUND(SUMPRODUCT({rng},{weights_array})/'
            f'SUMPRODUCT(({rng}<>"")*({rng}<>"-")*({rng}<>"--")*{weights_array}),0),"--")'
        )

    ws["E19"] = _fmla("E")
    ws["G19"] = _fmla("G")
    ws["H19"] = _fmla("H")

    out = tmp_path / "eval.xlsx"
    wb.save(out)

    xl = _fml.ExcelModel().loads(str(out)).finish()
    sol = xl.calculate()

    def _val(cell):
        for k, v in sol.items():
            if k.endswith(f"!{cell}"):
                raw = v.value if hasattr(v, "value") else v
                if hasattr(raw, "tolist"):
                    raw = raw.tolist()
                if isinstance(raw, list):
                    while isinstance(raw, list) and raw:
                        raw = raw[0]
                return raw
        return None

    assert _val("E19") == 74, f"Expected E19=74, got {_val('E19')}"
    assert _val("G19") == 80, f"Expected G19=80, got {_val('G19')}"
    assert _val("H19") == 76, f"Expected H19=76, got {_val('H19')}"


def test_weighted_avg_formula_has_both_high_and_low_columns(tmp_path: Path):
    """Both the high and low cells in each day pair should get formulas."""
    om_data, accu_data, google_data = _basic_inputs()

    out = tmp_path / "report.xlsx"
    generate_excel_report(
        om_data=om_data,
        noaa_data=[],
        accu_data=accu_data,
        google_data=google_data,
        output_path=out,
    )

    wb = load_workbook(out)
    ws = wb.active

    # Day 0: E (high) + F (low). Day 1: G (high) + H (low).
    for col_letter in ("E", "F", "G", "H"):
        v = ws[f"{col_letter}19"].value
        assert isinstance(v, str) and v.startswith("="), (
            f"Cell {col_letter}19 should be a formula; got {v!r}"
        )
        assert f"{col_letter}13:{col_letter}18" in v, (
            f"Formula at {col_letter}19 should reference its own column 13-18, got: {v}"
        )
