"""Tests for dashboard/app.py.

Covers all pure (non-Streamlit) functions: _categorize_reason,
filter_quarantine, filter_consolidated, and build_per_file_quality_table.
Render functions and cached loaders are excluded — they depend on a live
Streamlit runtime and are thin wrappers over src/ functions already tested
in tests/test_report.py and tests/test_export.py.

Importing app.py adds src/ to sys.path at module load time, so no
additional path setup is needed for src/ imports.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make dashboard/ importable; app.py adds src/ to sys.path on import.
_DASHBOARD_DIR = Path(__file__).resolve().parent.parent.parent / "dashboard"
sys.path.insert(0, str(_DASHBOARD_DIR))

from app import (
    _categorize_reason,
    build_per_file_quality_table,
    filter_consolidated,
    filter_quarantine,
)


# ---------------------------------------------------------------------------
# Shared test-data helpers
# ---------------------------------------------------------------------------


def _make_clean_df(
    n: int = 3,
    source_file: str = "sales.xlsx",
    region: str = "East",
) -> pd.DataFrame:
    """Return a minimal consolidated DataFrame with n rows.

    Args:
        n:           Number of rows to generate.
        source_file: Value for the source_file column.
        region:      Value for the region column.

    Returns:
        DataFrame with all canonical columns populated.
    """
    return pd.DataFrame({
        "source_file": [source_file] * n,
        "source_row":  list(range(2, n + 2)),
        "date":        ["2024-01-15"] * n,
        "product":     ["Widget A"] * n,
        "region":      [region] * n,
        "sales_rep":   ["Alice"] * n,
        "customer":    ["Acme Corp"] * n,
        "quantity":    [10.0] * n,
        "revenue":     [1000.0] * n,
    })


def _make_quarantine_df(
    n: int = 2,
    source_file: str = "sales.xlsx",
    reason: str = "revenue is negative (-50) in row 4 of sales.xlsx",
) -> pd.DataFrame:
    """Return a minimal quarantine DataFrame with n rows.

    Args:
        n:           Number of rows to generate.
        source_file: Value for the source_file column.
        reason:      Value for the quarantine_reason column.

    Returns:
        DataFrame with quarantine_reason, source_file, and data columns.
    """
    return pd.DataFrame({
        "quarantine_reason": [reason] * n,
        "source_file":       [source_file] * n,
        "source_row":        list(range(10, n + 10)),
        "date":              ["2024-03-01"] * n,
        "product":           ["Widget C"] * n,
        "region":            ["North"] * n,
        "quantity":          ["3.0"] * n,
        "revenue":           ["-50.0"] * n,
    })


# ---------------------------------------------------------------------------
# _categorize_reason
# ---------------------------------------------------------------------------


class TestCategorizeReason:
    """Tests for _categorize_reason."""

    def test_negative_value(self) -> None:
        """Reason containing 'is negative' maps to 'negative value'."""
        assert (
            _categorize_reason("revenue is negative (-450) in row 23 of Q1.xlsx")
            == "negative value"
        )

    def test_missing_required_field(self) -> None:
        """Reason containing 'required field' maps to 'missing required field'."""
        assert (
            _categorize_reason(
                "required field 'customer' is empty in row 5 of sales.xlsx"
            )
            == "missing required field"
        )

    def test_type_mismatch(self) -> None:
        """Reason containing 'is not a valid number' maps to 'type mismatch'."""
        assert (
            _categorize_reason("quantity 'TBD' is not a valid number in row 9")
            == "type mismatch"
        )

    def test_invalid_date(self) -> None:
        """Reason containing 'is not a valid date' maps to 'invalid date'."""
        assert (
            _categorize_reason("date '2024-13-01' is not a valid date in row 3")
            == "invalid date"
        )

    def test_out_of_range_below_minimum(self) -> None:
        """Reason containing 'is below minimum' maps to 'out of range'."""
        assert (
            _categorize_reason("quantity 0 is below minimum 1 in row 7")
            == "out of range"
        )

    def test_out_of_range_exceeds_maximum(self) -> None:
        """Reason containing 'exceeds maximum' maps to 'out of range'."""
        assert (
            _categorize_reason("revenue 999999 exceeds maximum in row 2")
            == "out of range"
        )

    def test_out_of_range_date_before_minimum(self) -> None:
        """Reason containing 'is before minimum' maps to 'out of range'."""
        assert (
            _categorize_reason("date '2010-01-01' is before minimum 2015-01-01")
            == "out of range"
        )

    def test_out_of_range_date_after_maximum(self) -> None:
        """Reason containing 'is after maximum' maps to 'out of range'."""
        assert (
            _categorize_reason("date '2030-01-01' is after maximum 2026-12-31")
            == "out of range"
        )

    def test_pattern_mismatch(self) -> None:
        """Reason containing 'does not match pattern' maps to 'pattern mismatch'."""
        assert (
            _categorize_reason("email 'notanemail' does not match pattern")
            == "pattern mismatch"
        )

    def test_sparse_row(self) -> None:
        """Reason containing 'too few non-null' maps to 'sparse row'."""
        assert (
            _categorize_reason("too few non-null fields in row 15") == "sparse row"
        )

    def test_unknown_reason_returns_other(self) -> None:
        """Unrecognised reason string maps to 'other'."""
        assert _categorize_reason("something completely different") == "other"

    def test_empty_string_returns_other(self) -> None:
        """Empty string maps to 'other'."""
        assert _categorize_reason("") == "other"

    def test_case_insensitive(self) -> None:
        """Matching is case-insensitive."""
        assert (
            _categorize_reason("Revenue IS NEGATIVE (-10) in row 1") == "negative value"
        )

    def test_first_match_wins(self) -> None:
        """When multiple keywords match, the first rule in _REASON_KEYWORDS wins."""
        # 'required field' appears before 'is negative' in _REASON_KEYWORDS.
        reason = "required field 'revenue' is empty and is negative in row 5"
        assert _categorize_reason(reason) == "missing required field"


# ---------------------------------------------------------------------------
# filter_quarantine
# ---------------------------------------------------------------------------


class TestFilterQuarantine:
    """Tests for filter_quarantine."""

    def test_no_filters_returns_all_rows(self) -> None:
        """Both filters None returns the full DataFrame."""
        df = _make_quarantine_df(n=4)
        result = filter_quarantine(df, source_file=None, reason_type=None)
        assert len(result) == 4

    def test_filter_by_source_file(self) -> None:
        """Only rows matching the given source_file are returned."""
        df = pd.concat([
            _make_quarantine_df(n=2, source_file="a.xlsx"),
            _make_quarantine_df(n=3, source_file="b.xlsx"),
        ], ignore_index=True)
        result = filter_quarantine(df, source_file="a.xlsx", reason_type=None)
        assert len(result) == 2
        assert (result["source_file"] == "a.xlsx").all()

    def test_filter_by_reason_type(self) -> None:
        """Only rows whose quarantine_reason maps to the given category are returned."""
        negative = "revenue is negative (-10) in row 1 of a.xlsx"
        missing = "required field 'customer' is empty in row 2 of a.xlsx"
        df = pd.DataFrame({
            "quarantine_reason": [negative, missing, negative],
            "source_file": ["a.xlsx"] * 3,
        })
        result = filter_quarantine(df, source_file=None, reason_type="negative value")
        assert len(result) == 2

    def test_filter_by_both_file_and_reason(self) -> None:
        """Both filters applied together: file AND reason must match."""
        df = pd.DataFrame({
            "quarantine_reason": [
                "revenue is negative (-10) in row 1",
                "revenue is negative (-20) in row 2",
                "required field 'customer' is empty in row 3",
            ],
            "source_file": ["a.xlsx", "b.xlsx", "a.xlsx"],
        })
        result = filter_quarantine(df, source_file="a.xlsx", reason_type="negative value")
        assert len(result) == 1
        assert result.iloc[0]["source_file"] == "a.xlsx"

    def test_empty_dataframe_returns_empty(self) -> None:
        """Empty input returns an empty DataFrame without error."""
        df = pd.DataFrame(columns=["quarantine_reason", "source_file"])
        result = filter_quarantine(df, source_file="a.xlsx", reason_type="negative value")
        assert result.empty

    def test_nonexistent_source_file_returns_empty(self) -> None:
        """A source_file value not present in the data returns empty."""
        df = _make_quarantine_df(n=3, source_file="real.xlsx")
        result = filter_quarantine(df, source_file="ghost.xlsx", reason_type=None)
        assert result.empty

    def test_nonexistent_reason_type_returns_empty(self) -> None:
        """A reason_type category not present in the data returns empty."""
        df = _make_quarantine_df(n=3, reason="revenue is negative (-10) in row 1")
        result = filter_quarantine(df, source_file=None, reason_type="pattern mismatch")
        assert result.empty

    def test_original_dataframe_not_mutated(self) -> None:
        """The input DataFrame is not modified by filtering."""
        df = _make_quarantine_df(n=4)
        original_len = len(df)
        filter_quarantine(df, source_file="ghost.xlsx", reason_type=None)
        assert len(df) == original_len

    def test_null_reason_treated_as_other(self) -> None:
        """A null quarantine_reason is treated as 'other' by _categorize_reason."""
        df = pd.DataFrame({
            "quarantine_reason": [None, "revenue is negative (-5) in row 1"],
            "source_file": ["a.xlsx", "a.xlsx"],
        })
        result = filter_quarantine(df, source_file=None, reason_type="other")
        assert len(result) == 1
        assert result.iloc[0]["quarantine_reason"] is None


# ---------------------------------------------------------------------------
# filter_consolidated
# ---------------------------------------------------------------------------


class TestFilterConsolidated:
    """Tests for filter_consolidated."""

    def test_no_filters_returns_all_rows(self) -> None:
        """All filters None returns the full DataFrame."""
        df = _make_clean_df(n=5)
        result = filter_consolidated(df, source_file=None, region=None, date_range=None)
        assert len(result) == 5

    def test_filter_by_source_file(self) -> None:
        """Only rows matching the given source_file are returned."""
        df = pd.concat([
            _make_clean_df(n=3, source_file="q1.xlsx"),
            _make_clean_df(n=2, source_file="q2.xlsx"),
        ], ignore_index=True)
        result = filter_consolidated(df, source_file="q1.xlsx", region=None, date_range=None)
        assert len(result) == 3
        assert (result["source_file"] == "q1.xlsx").all()

    def test_filter_by_region(self) -> None:
        """Only rows matching the given region are returned."""
        df = pd.concat([
            _make_clean_df(n=4, region="East"),
            _make_clean_df(n=2, region="West"),
        ], ignore_index=True)
        result = filter_consolidated(df, source_file=None, region="East", date_range=None)
        assert len(result) == 4
        assert (result["region"] == "East").all()

    def test_filter_by_date_range_inclusive(self) -> None:
        """Both endpoints of the date range are included in results."""
        df = pd.DataFrame({
            "source_file": ["a.xlsx"] * 4,
            "date":        ["2024-01-01", "2024-06-15", "2024-12-31", "2025-01-01"],
            "region":      ["East"] * 4,
        })
        result = filter_consolidated(
            df, source_file=None, region=None,
            date_range=("2024-01-01", "2024-12-31"),
        )
        assert len(result) == 3
        assert "2025-01-01" not in result["date"].values

    def test_filter_date_range_excludes_outside(self) -> None:
        """Dates outside the range boundaries are excluded."""
        df = pd.DataFrame({
            "source_file": ["a.xlsx"] * 3,
            "date":        ["2023-12-31", "2024-06-01", "2025-01-01"],
            "region":      ["East"] * 3,
        })
        result = filter_consolidated(
            df, source_file=None, region=None,
            date_range=("2024-01-01", "2024-12-31"),
        )
        assert len(result) == 1
        assert result.iloc[0]["date"] == "2024-06-01"

    def test_combined_file_and_region_filter(self) -> None:
        """Source file and region filters are applied together (AND logic)."""
        df = pd.DataFrame({
            "source_file": ["a.xlsx", "a.xlsx", "b.xlsx", "b.xlsx"],
            "date":        ["2024-01-01"] * 4,
            "region":      ["East", "West", "East", "East"],
        })
        result = filter_consolidated(
            df, source_file="a.xlsx", region="East", date_range=None
        )
        assert len(result) == 1
        assert result.iloc[0]["source_file"] == "a.xlsx"
        assert result.iloc[0]["region"] == "East"

    def test_empty_dataframe_returns_empty(self) -> None:
        """Empty input returns an empty DataFrame without error."""
        df = pd.DataFrame(columns=["source_file", "date", "region"])
        result = filter_consolidated(
            df, source_file="a.xlsx", region=None, date_range=None
        )
        assert result.empty

    def test_nonexistent_region_returns_empty(self) -> None:
        """A region value not present in the data returns empty."""
        df = _make_clean_df(n=3, region="East")
        result = filter_consolidated(
            df, source_file=None, region="North", date_range=None
        )
        assert result.empty

    def test_original_dataframe_not_mutated(self) -> None:
        """The input DataFrame is not modified by filtering."""
        df = _make_clean_df(n=5)
        original_len = len(df)
        filter_consolidated(df, source_file="ghost.xlsx", region=None, date_range=None)
        assert len(df) == original_len


# ---------------------------------------------------------------------------
# build_per_file_quality_table
# ---------------------------------------------------------------------------


class TestBuildPerFileQualityTable:
    """Tests for build_per_file_quality_table."""

    def test_output_columns(self) -> None:
        """Result has exactly the four expected columns in order."""
        result = build_per_file_quality_table(
            _make_clean_df(n=3), _make_quarantine_df(n=1)
        )
        assert list(result.columns) == [
            "source_file", "rows_loaded", "rows_quarantined", "quarantine_rate_pct"
        ]

    def test_row_counts_correct(self) -> None:
        """rows_loaded and rows_quarantined reflect the input DataFrames."""
        clean_df = _make_clean_df(n=8, source_file="sales.xlsx")
        quarantine_df = _make_quarantine_df(n=2, source_file="sales.xlsx")
        result = build_per_file_quality_table(clean_df, quarantine_df)
        row = result[result["source_file"] == "sales.xlsx"].iloc[0]
        assert row["rows_loaded"] == 8
        assert row["rows_quarantined"] == 2

    def test_quarantine_rate_calculation(self) -> None:
        """2 quarantine / 10 total = 20.0%."""
        clean_df = _make_clean_df(n=8, source_file="sales.xlsx")
        quarantine_df = _make_quarantine_df(n=2, source_file="sales.xlsx")
        result = build_per_file_quality_table(clean_df, quarantine_df)
        row = result[result["source_file"] == "sales.xlsx"].iloc[0]
        assert row["quarantine_rate_pct"] == 20.0

    def test_file_only_in_clean(self) -> None:
        """A file with no quarantine rows shows 0 quarantined and 0% rate."""
        clean_df = _make_clean_df(n=5, source_file="clean_only.xlsx")
        quarantine_df = pd.DataFrame(columns=["quarantine_reason", "source_file"])
        result = build_per_file_quality_table(clean_df, quarantine_df)
        row = result[result["source_file"] == "clean_only.xlsx"].iloc[0]
        assert row["rows_loaded"] == 5
        assert row["rows_quarantined"] == 0
        assert row["quarantine_rate_pct"] == 0.0

    def test_file_only_in_quarantine(self) -> None:
        """A file with no clean rows shows 0 loaded and 100% rate."""
        clean_df = pd.DataFrame(columns=["source_file"])
        quarantine_df = _make_quarantine_df(n=3, source_file="bad_data.xlsx")
        result = build_per_file_quality_table(clean_df, quarantine_df)
        row = result[result["source_file"] == "bad_data.xlsx"].iloc[0]
        assert row["rows_loaded"] == 0
        assert row["rows_quarantined"] == 3
        assert row["quarantine_rate_pct"] == 100.0

    def test_multiple_files_all_present(self) -> None:
        """Every distinct source_file gets its own row in the output."""
        clean_df = pd.concat([
            _make_clean_df(n=5, source_file="a.xlsx"),
            _make_clean_df(n=3, source_file="b.xlsx"),
        ], ignore_index=True)
        quarantine_df = _make_quarantine_df(n=1, source_file="a.xlsx")
        result = build_per_file_quality_table(clean_df, quarantine_df)
        assert len(result) == 2
        assert set(result["source_file"]) == {"a.xlsx", "b.xlsx"}

    def test_sorted_by_source_file(self) -> None:
        """Output rows are sorted ascending by source_file."""
        clean_df = pd.concat([
            _make_clean_df(n=2, source_file="z.xlsx"),
            _make_clean_df(n=2, source_file="a.xlsx"),
        ], ignore_index=True)
        quarantine_df = pd.DataFrame(columns=["quarantine_reason", "source_file"])
        result = build_per_file_quality_table(clean_df, quarantine_df)
        assert result["source_file"].tolist() == ["a.xlsx", "z.xlsx"]

    def test_both_empty_returns_empty(self) -> None:
        """Both inputs empty returns an empty DataFrame without error."""
        clean_df = pd.DataFrame(columns=["source_file"])
        quarantine_df = pd.DataFrame(columns=["quarantine_reason", "source_file"])
        result = build_per_file_quality_table(clean_df, quarantine_df)
        assert result.empty

    def test_no_zero_division_error(self) -> None:
        """The replace(0, 1) guard prevents ZeroDivisionError on empty inputs."""
        clean_df = pd.DataFrame({"source_file": []})
        quarantine_df = pd.DataFrame({"quarantine_reason": [], "source_file": []})
        result = build_per_file_quality_table(clean_df, quarantine_df)
        assert isinstance(result, pd.DataFrame)
