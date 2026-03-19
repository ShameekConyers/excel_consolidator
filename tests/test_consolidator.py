"""Tests for src/consolidator.py.

Covers unit tests for every public function and selected private helpers.
Integration-level tests use tmp_path CSV files to exercise consolidate()
and load_file() end-to-end without requiring real Excel fixtures.
"""

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from consolidator import (
    CleaningEntry,
    _coerce_numeric,
    _promote_header,
    clean_numeric_columns,
    consolidate,
    detect_header_row,
    handle_missing_values,
    load_file,
    log_action,
    normalize_dates,
    remove_duplicates,
    standardize_columns,
    tag_source,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_log() -> list[CleaningEntry]:
    """Return an empty cleaning log."""
    return []


def _write_csv(path: Path, content: str) -> Path:
    """Write *content* to *path* and return the path."""
    path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# detect_header_row
# ---------------------------------------------------------------------------


class TestDetectHeaderRow:
    """Tests for detect_header_row."""

    def test_header_at_row_zero(self) -> None:
        """Header row is the first row — returns 0."""
        raw = pd.DataFrame(
            [
                ["date", "product", "region", "quantity", "revenue"],
                ["2024-01-01", "Widget", "North", "5", "100"],
            ]
        )
        assert detect_header_row(raw) == 0

    def test_header_after_title_rows(self) -> None:
        """Two title rows above header — returns 2."""
        raw = pd.DataFrame(
            [
                ["Q1 Sales Report", None, None, None, None],
                [None, None, None, None, None],
                ["date", "product", "region", "quantity", "revenue"],
                ["2024-01-01", "Widget", "North", "5", "100"],
            ]
        )
        assert detect_header_row(raw) == 2

    def test_fallback_to_zero_when_no_match(self) -> None:
        """No recognisable header row → falls back to 0."""
        raw = pd.DataFrame(
            [
                ["foo", "bar", "baz"],
                ["a", "b", "c"],
            ]
        )
        assert detect_header_row(raw) == 0

    def test_below_threshold_falls_back(self) -> None:
        """Fewer than THRESHOLD matches in every row → returns 0."""
        # Only two known columns (date, product) — threshold is 3
        raw = pd.DataFrame(
            [
                ["date", "product", "junk"],
                ["2024-01-01", "Widget", "x"],
            ]
        )
        assert detect_header_row(raw) == 0

    def test_header_at_threshold_exact(self) -> None:
        """Exactly three matches on row 1 meets threshold."""
        raw = pd.DataFrame(
            [
                ["garbage row", None, None, None],
                ["date", "product", "region", None],
                ["2024-01-01", "Widget", "North", "50"],
            ]
        )
        assert detect_header_row(raw) == 1


# ---------------------------------------------------------------------------
# standardize_columns
# ---------------------------------------------------------------------------


class TestStandardizeColumns:
    """Tests for standardize_columns."""

    def test_renames_known_variants(self) -> None:
        """Variant column names are mapped to canonical names."""
        df = pd.DataFrame({"Transaction Date": [], "Rev.": [], "Qty": []})
        log = make_log()
        result = standardize_columns(df, "test.csv", log)
        assert "date" in result.columns
        assert "revenue" in result.columns
        assert "quantity" in result.columns

    def test_logs_rename_entries(self) -> None:
        """A CleaningEntry is appended for each renamed column."""
        df = pd.DataFrame({"Rev.": [], "Qty": []})
        log = make_log()
        standardize_columns(df, "test.csv", log)
        transformations = [e.transformation for e in log]
        assert transformations.count("rename_column") == 2

    def test_leaves_unknown_columns_unchanged(self) -> None:
        """Columns with no mapping are preserved as-is."""
        df = pd.DataFrame({"date": [], "notes": []})
        log = make_log()
        result = standardize_columns(df, "test.csv", log)
        assert "notes" in result.columns

    def test_no_log_when_already_canonical(self) -> None:
        """Already-canonical names produce no log entries."""
        df = pd.DataFrame({"date": [], "revenue": []})
        log = make_log()
        standardize_columns(df, "test.csv", log)
        assert len(log) == 0

    def test_case_insensitive_matching(self) -> None:
        """Column matching is case-insensitive."""
        df = pd.DataFrame({"REVENUE": [], "DATE": []})
        log = make_log()
        result = standardize_columns(df, "test.csv", log)
        assert "revenue" in result.columns
        assert "date" in result.columns

    def test_non_string_columns_skipped(self) -> None:
        """Non-string column labels (e.g. integers from header=None reads) are skipped."""
        df = pd.DataFrame({0: [], 1: []})
        log = make_log()
        result = standardize_columns(df, "test.csv", log)
        assert list(result.columns) == [0, 1]
        assert len(log) == 0


# ---------------------------------------------------------------------------
# normalize_dates
# ---------------------------------------------------------------------------


class TestNormalizeDates:
    """Tests for normalize_dates."""

    def test_iso_format_unchanged(self) -> None:
        """ISO dates are already correct and still pass through as YYYY-MM-DD."""
        df = pd.DataFrame({"date": ["2024-01-15"]})
        log = make_log()
        result = normalize_dates(df, "test.csv", log)
        assert result["date"].iloc[0] == "2024-01-15"

    def test_us_format_converted(self) -> None:
        """MM/DD/YYYY format is converted to YYYY-MM-DD."""
        df = pd.DataFrame({"date": ["03/15/2024"]})
        log = make_log()
        result = normalize_dates(df, "test.csv", log)
        assert result["date"].iloc[0] == "2024-03-15"

    def test_written_month_converted(self) -> None:
        """'March 15, 2024' is converted to YYYY-MM-DD."""
        df = pd.DataFrame({"date": ["March 15, 2024"]})
        log = make_log()
        result = normalize_dates(df, "test.csv", log)
        assert result["date"].iloc[0] == "2024-03-15"

    def test_unparseable_left_unchanged(self) -> None:
        """Values that cannot be parsed are preserved for the validator."""
        df = pd.DataFrame({"date": ["not-a-date"]})
        log = make_log()
        result = normalize_dates(df, "test.csv", log)
        assert result["date"].iloc[0] == "not-a-date"

    def test_no_date_column_returns_unchanged(self) -> None:
        """DataFrame without a 'date' column is returned as-is."""
        df = pd.DataFrame({"revenue": ["100"]})
        log = make_log()
        result = normalize_dates(df, "test.csv", log)
        assert list(result.columns) == ["revenue"]
        assert len(log) == 0

    def test_log_entry_on_conversion(self) -> None:
        """A log entry is created when dates are converted."""
        df = pd.DataFrame({"date": ["03/15/2024"]})
        log = make_log()
        normalize_dates(df, "test.csv", log)
        assert any(e.transformation == "normalize_date" for e in log)


# ---------------------------------------------------------------------------
# clean_numeric_columns
# ---------------------------------------------------------------------------


class TestCleanNumericColumns:
    """Tests for clean_numeric_columns."""

    def test_strips_dollar_sign_and_commas(self) -> None:
        """'$3,000' becomes '3000.0'."""
        df = pd.DataFrame({"revenue": ["$3,000"]})
        log = make_log()
        result = clean_numeric_columns(df, "test.csv", log)
        assert result["revenue"].iloc[0] == "3000.0"

    def test_plain_number_normalized_to_float_string(self) -> None:
        """A plain integer string '1500' is normalized to the float string '1500.0'."""
        df = pd.DataFrame({"revenue": ["1500"]})
        log = make_log()
        result = clean_numeric_columns(df, "test.csv", log)
        assert float(result["revenue"].iloc[0]) == 1500.0

    def test_non_numeric_string_preserved(self) -> None:
        """'pending' is left unchanged so the validator can quarantine it."""
        df = pd.DataFrame({"quantity": ["pending"]})
        log = make_log()
        result = clean_numeric_columns(df, "test.csv", log)
        assert result["quantity"].iloc[0] == "pending"

    def test_negative_value_preserved(self) -> None:
        """Negative values are converted but sign is preserved."""
        df = pd.DataFrame({"revenue": ["-450.00"]})
        log = make_log()
        result = clean_numeric_columns(df, "test.csv", log)
        assert float(result["revenue"].iloc[0]) == -450.0

    def test_column_not_present_skipped(self) -> None:
        """DataFrame without quantity/revenue columns produces no log entries."""
        df = pd.DataFrame({"product": ["Widget"]})
        log = make_log()
        clean_numeric_columns(df, "test.csv", log)
        assert len(log) == 0

    def test_logs_when_values_cleaned(self) -> None:
        """A log entry is written when at least one value is stripped."""
        df = pd.DataFrame({"revenue": ["$1,000"]})
        log = make_log()
        clean_numeric_columns(df, "test.csv", log)
        assert any(e.transformation == "strip_currency_symbols" for e in log)


# ---------------------------------------------------------------------------
# remove_duplicates
# ---------------------------------------------------------------------------


class TestRemoveDuplicates:
    """Tests for remove_duplicates."""

    def test_removes_exact_cross_file_duplicates(self) -> None:
        """Duplicate data rows (same data, different source) are reduced to one."""
        df = pd.DataFrame(
            {
                "source_file": ["a.csv", "b.csv"],
                "source_row": [2, 2],
                "date": ["2024-01-01", "2024-01-01"],
                "revenue": ["100", "100"],
            }
        )
        log = make_log()
        result = remove_duplicates(df, log)
        assert len(result) == 1
        assert any(e.transformation == "remove_exact_duplicates" for e in log)

    def test_no_duplicates_unchanged(self) -> None:
        """DataFrame with distinct rows is returned unchanged."""
        df = pd.DataFrame(
            {
                "source_file": ["a.csv", "a.csv"],
                "source_row": [2, 3],
                "date": ["2024-01-01", "2024-01-02"],
                "revenue": ["100", "200"],
            }
        )
        log = make_log()
        result = remove_duplicates(df, log)
        assert len(result) == 2
        assert len(log) == 0

    def test_first_occurrence_kept(self) -> None:
        """The first occurrence of a duplicate is kept, not the second."""
        df = pd.DataFrame(
            {
                "source_file": ["first.csv", "second.csv"],
                "source_row": [2, 2],
                "value": ["same", "same"],
            }
        )
        log = make_log()
        result = remove_duplicates(df, log)
        assert result.iloc[0]["source_file"] == "first.csv"


# ---------------------------------------------------------------------------
# handle_missing_values
# ---------------------------------------------------------------------------


class TestHandleMissingValues:
    """Tests for handle_missing_values."""

    def _base_df(self) -> pd.DataFrame:
        """Return a DataFrame with one fully-empty and one valid row."""
        return pd.DataFrame(
            {
                "source_file": ["test.csv", "test.csv"],
                "source_row": [2, 3],
                "date": [None, "2024-01-01"],
                "revenue": [None, "100"],
            }
        )

    def test_drop_empty_removes_fully_empty_rows(self) -> None:
        """Fully-empty rows are dropped under the default strategy."""
        df = self._base_df()
        log = make_log()
        result = handle_missing_values(df, "test.csv", log, strategy="drop_empty")
        assert len(result) == 1
        assert result.iloc[0]["revenue"] == "100"

    def test_drop_empty_logs_action(self) -> None:
        """Log entry is written when rows are dropped."""
        df = self._base_df()
        log = make_log()
        handle_missing_values(df, "test.csv", log)
        assert any(e.transformation == "drop_empty_rows" for e in log)

    def test_flag_strategy_adds_column(self) -> None:
        """Flag strategy adds '_all_empty' column instead of dropping."""
        df = self._base_df()
        log = make_log()
        result = handle_missing_values(df, "test.csv", log, strategy="flag")
        assert "_all_empty" in result.columns
        assert len(result) == 2  # rows not dropped

    def test_flag_strategy_marks_correct_rows(self) -> None:
        """Only the fully-empty row is flagged True."""
        df = self._base_df()
        log = make_log()
        result = handle_missing_values(df, "test.csv", log, strategy="flag")
        assert result["_all_empty"].tolist() == [True, False]

    def test_invalid_strategy_raises(self) -> None:
        """An unsupported strategy string raises ValueError."""
        df = self._base_df()
        log = make_log()
        with pytest.raises(ValueError, match="Unknown missing-value strategy"):
            handle_missing_values(df, "test.csv", log, strategy="invalid")

    def test_partial_row_preserved(self) -> None:
        """A row with at least one non-empty data column is not dropped."""
        df = pd.DataFrame(
            {
                "source_file": ["test.csv"],
                "source_row": [2],
                "date": ["2024-01-01"],
                "revenue": [None],
            }
        )
        log = make_log()
        result = handle_missing_values(df, "test.csv", log)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# tag_source
# ---------------------------------------------------------------------------


class TestTagSource:
    """Tests for tag_source."""

    def test_prepends_source_columns(self) -> None:
        """source_file and source_row are the first two columns."""
        df = pd.DataFrame({"revenue": ["100", "200"]})
        result = tag_source(df, Path("data/sales.csv"), header_row_idx=0)
        assert list(result.columns[:2]) == ["source_file", "source_row"]

    def test_source_file_is_basename(self) -> None:
        """source_file contains only the filename, not the full path."""
        df = pd.DataFrame({"revenue": ["100"]})
        result = tag_source(df, Path("/long/path/sales.csv"), header_row_idx=0)
        assert result["source_file"].iloc[0] == "sales.csv"

    def test_source_row_starts_at_correct_offset(self) -> None:
        """With no title rows (header_idx=0) first data row is Excel row 2."""
        df = pd.DataFrame({"revenue": ["100", "200"]})
        result = tag_source(df, Path("sales.csv"), header_row_idx=0)
        assert result["source_row"].tolist() == [2, 3]

    def test_source_row_with_title_rows(self) -> None:
        """With two title rows (header_idx=2) first data row is Excel row 4."""
        df = pd.DataFrame({"revenue": ["100", "200"]})
        result = tag_source(df, Path("sales.csv"), header_row_idx=2)
        assert result["source_row"].tolist() == [4, 5]


# ---------------------------------------------------------------------------
# log_action
# ---------------------------------------------------------------------------


class TestLogAction:
    """Tests for log_action."""

    def test_appends_entry(self) -> None:
        """Calling log_action appends exactly one CleaningEntry."""
        log = make_log()
        log_action(log, "test.csv", "rename_column", "Rev.", "revenue")
        assert len(log) == 1

    def test_entry_fields_correct(self) -> None:
        """The appended entry has the expected field values."""
        log = make_log()
        log_action(log, "test.csv", "rename_column", "Rev.", "revenue")
        entry = log[0]
        assert entry.source_file == "test.csv"
        assert entry.transformation == "rename_column"
        assert entry.original_value == "Rev."
        assert entry.new_value == "revenue"

    def test_timestamp_is_set(self) -> None:
        """The entry has a non-empty timestamp string."""
        log = make_log()
        log_action(log, "test.csv", "rename_column", "Rev.", "revenue")
        assert log[0].timestamp != ""


# ---------------------------------------------------------------------------
# _coerce_numeric (private helper, tested directly)
# ---------------------------------------------------------------------------


class TestCoerceNumeric:
    """Tests for the private _coerce_numeric helper."""

    def test_dollar_and_commas(self) -> None:
        """'$3,000' → '3000.0'."""
        assert _coerce_numeric("$3,000") == "3000.0"

    def test_plain_number(self) -> None:
        """'1500' → '1500.0'."""
        assert _coerce_numeric("1500") == "1500.0"

    def test_negative_number(self) -> None:
        """'-450.00' → '-450.0'."""
        assert _coerce_numeric("-450.00") == "-450.0"

    def test_non_numeric_string_unchanged(self) -> None:
        """'pending' is returned unchanged."""
        assert _coerce_numeric("pending") == "pending"

    def test_none_returned_as_is(self) -> None:
        """None input → None output."""
        assert _coerce_numeric(None) is None

    def test_nan_returned_as_is(self) -> None:
        """float NaN input → NaN output."""
        result = _coerce_numeric(float("nan"))
        assert isinstance(result, float) and math.isnan(result)


# ---------------------------------------------------------------------------
# load_file (integration — uses tmp_path CSV)
# ---------------------------------------------------------------------------


class TestLoadFile:
    """Integration tests for load_file using temporary CSV files."""

    def test_loads_simple_csv(self, tmp_path: Path) -> None:
        """A well-formed CSV is loaded and returns a DataFrame with canonical cols."""
        csv = _write_csv(
            tmp_path / "sales.csv",
            "date,product,region,sales_rep,customer,quantity,revenue\n"
            "2024-01-01,Widget,North,Alice,Acme,5,500\n",
        )
        log = make_log()
        df = load_file(csv, log)
        assert "date" in df.columns
        assert "revenue" in df.columns
        assert len(df) == 1

    def test_injects_west_region_for_west_filename(self, tmp_path: Path) -> None:
        """A file whose name contains 'west' gets region='West' injected."""
        csv = _write_csv(
            tmp_path / "west_region_2024.csv",
            "date,product,sales_rep,customer,quantity,revenue\n"
            "2024-01-01,Widget,Alice,Acme,5,500\n",
        )
        log = make_log()
        df = load_file(csv, log)
        assert "region" in df.columns
        assert df["region"].iloc[0] == "West"
        assert any(e.transformation == "inject_region" for e in log)

    def test_source_columns_added(self, tmp_path: Path) -> None:
        """source_file and source_row columns are present after load_file."""
        csv = _write_csv(
            tmp_path / "sales.csv",
            "date,product,region,sales_rep,customer,quantity,revenue\n"
            "2024-01-01,Widget,North,Alice,Acme,5,500\n",
        )
        log = make_log()
        df = load_file(csv, log)
        assert "source_file" in df.columns
        assert "source_row" in df.columns

    def test_variant_column_names_standardized(self, tmp_path: Path) -> None:
        """Column variants (e.g. 'Rev.', 'Qty') are mapped to canonical names."""
        csv = _write_csv(
            tmp_path / "sales.csv",
            "date,product line,territory,rep name,client,units,rev.\n"
            "2024-01-01,Widget,North,Alice,Acme,5,500\n",
        )
        log = make_log()
        df = load_file(csv, log)
        for col in ("product", "region", "sales_rep", "customer", "quantity", "revenue"):
            assert col in df.columns, f"Missing canonical column: {col}"

    def test_invalid_extension_raises(self, tmp_path: Path) -> None:
        """A file with an unsupported extension raises ValueError."""
        bad = tmp_path / "data.txt"
        bad.write_text("nothing\n")
        log = make_log()
        with pytest.raises(ValueError, match="Unsupported file type"):
            load_file(bad, log)


# ---------------------------------------------------------------------------
# consolidate (integration — uses tmp_path CSV files)
# ---------------------------------------------------------------------------


class TestConsolidate:
    """Integration tests for consolidate using temporary directories."""

    def test_merges_multiple_files(self, tmp_path: Path) -> None:
        """Two CSV files are concatenated into a single DataFrame."""
        header = "date,product,region,sales_rep,customer,quantity,revenue\n"
        _write_csv(tmp_path / "a.csv", header + "2024-01-01,Widget,North,Alice,Acme,5,500\n")
        _write_csv(tmp_path / "b.csv", header + "2024-01-02,Gadget,South,Bob,Corp,3,300\n")
        df, log = consolidate(tmp_path)
        assert len(df) == 2

    def test_returns_cleaning_log(self, tmp_path: Path) -> None:
        """consolidate returns a non-None cleaning log list."""
        header = "date,product,region,sales_rep,customer,quantity,revenue\n"
        _write_csv(tmp_path / "a.csv", header + "2024-01-01,Widget,North,Alice,Acme,5,500\n")
        df, log = consolidate(tmp_path)
        assert isinstance(log, list)

    def test_skips_lock_files(self, tmp_path: Path) -> None:
        """Files prefixed with '~$' (Excel auto-save) are silently ignored."""
        header = "date,product,region,sales_rep,customer,quantity,revenue\n"
        _write_csv(tmp_path / "sales.csv", header + "2024-01-01,Widget,North,Alice,Acme,5,500\n")
        _write_csv(tmp_path / "~$sales.csv", "garbage lock file content\n")
        df, _ = consolidate(tmp_path)
        assert len(df) == 1

    def test_raises_on_empty_folder(self, tmp_path: Path) -> None:
        """An empty folder raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="No Excel/CSV files found"):
            consolidate(tmp_path)

    def test_cross_file_duplicates_removed(self, tmp_path: Path) -> None:
        """The same row appearing in two files is deduplicated."""
        header = "date,product,region,sales_rep,customer,quantity,revenue\n"
        row = "2024-01-01,Widget,North,Alice,Acme,5,500\n"
        _write_csv(tmp_path / "a.csv", header + row)
        _write_csv(tmp_path / "b.csv", header + row)
        df, log = consolidate(tmp_path)
        assert len(df) == 1
        assert any(e.transformation == "remove_exact_duplicates" for e in log)
