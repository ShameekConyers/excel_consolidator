"""Tests for src/report.py.

Covers unit tests for every public function and all private helpers.
Integration-level tests use tmp_path directories with real SQLite databases
written directly via sqlite3.
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from report import (
    CleaningSummary,
    QuarantineSummary,
    ReportResult,
    _classify_reason,
    _collect_file_names,
    _count_transformations,
    _drop_db_cols,
    _parse_leading_int,
    _sum_leading_ints,
    generate_cleaning_summary,
    generate_quarantine_summary,
    read_cleaning_log,
    read_consolidated,
    read_quarantine,
    render_markdown,
    render_terminal,
    report,
)


# ---------------------------------------------------------------------------
# Shared test-data helpers
# ---------------------------------------------------------------------------


def _make_clean_df(
    n: int = 3,
    source_file: str = "sales.xlsx",
) -> pd.DataFrame:
    """Return a minimal clean DataFrame with n rows and a source_file column."""
    return pd.DataFrame({
        "source_file": [source_file] * n,
        "source_row":  list(range(2, n + 2)),
        "date":        ["2024-01-15"] * n,
        "product":     ["Widget A"] * n,
        "region":      ["East"] * n,
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
    """Return a minimal quarantine DataFrame with n rows."""
    return pd.DataFrame({
        "quarantine_reason": [reason] * n,
        "source_file":       [source_file] * n,
        "source_row":        list(range(10, n + 10)),
        "date":              ["2024-03-01"] * n,
        "product":           ["Widget C"] * n,
        "region":            ["North"] * n,
        "sales_rep":         ["Bob"] * n,
        "customer":          ["Corp Inc"] * n,
        "quantity":          ["3.0"] * n,
        "revenue":           ["-50.0"] * n,
    })


def _make_log_df(entries: list[tuple[str, str, str, str]] | None = None) -> pd.DataFrame:
    """Return a cleaning log DataFrame.

    Args:
        entries: List of (source_file, transformation, original_value, new_value)
                 tuples. Defaults to a small set covering the main transformation
                 types used in the report.
    """
    if entries is None:
        entries = [
            ("sales.xlsx", "rename_column", "Rev.", "revenue"),
            ("sales.xlsx", "rename_column", "Qty", "quantity"),
            ("sales.xlsx", "normalize_date", "mixed date formats", "14 value(s) converted to YYYY-MM-DD"),
            ("sales.xlsx", "strip_currency_symbols", "3 value(s) in 'revenue' had symbols/commas", "stripped and stored as plain numeric string"),
            ("(all files)", "remove_exact_duplicates", "2 duplicate row(s) found across files", "removed — kept first occurrence per file sort order"),
        ]
    return pd.DataFrame(entries, columns=["source_file", "transformation", "original_value", "new_value"])


def _make_db(
    tmp_path: Path,
    n_clean: int = 3,
    n_quarantine: int = 2,
    n_log: int = 3,
    clean_file: str = "sales.xlsx",
    quarantine_file: str = "sales.xlsx",
    quarantine_reason: str = "revenue is negative (-50) in row 10 of sales.xlsx",
) -> Path:
    """Create a minimal pipeline SQLite database with all three tables.

    Args:
        tmp_path:          pytest tmp_path directory.
        n_clean:           Rows to insert into the consolidated table.
        n_quarantine:      Rows to insert into the quarantine table.
        n_log:             Rows to insert into the cleaning_log table.
        clean_file:        source_file value for consolidated rows.
        quarantine_file:   source_file value for quarantine rows.
        quarantine_reason: quarantine_reason value for all quarantine rows.

    Returns:
        Path to the created SQLite file.
    """
    db = tmp_path / "test.db"
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE consolidated (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT,
                source_row  INTEGER,
                date        TEXT,
                product     TEXT,
                region      TEXT,
                sales_rep   TEXT,
                customer    TEXT,
                quantity    REAL,
                revenue     REAL,
                loaded_at   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
            CREATE TABLE quarantine (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                quarantine_reason TEXT NOT NULL,
                source_file       TEXT,
                source_row        INTEGER,
                date              TEXT,
                product           TEXT,
                region            TEXT,
                sales_rep         TEXT,
                customer          TEXT,
                quantity          TEXT,
                revenue           TEXT,
                quarantined_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
            CREATE TABLE cleaning_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file    TEXT NOT NULL,
                transformation TEXT NOT NULL,
                original_value TEXT NOT NULL,
                new_value      TEXT NOT NULL,
                timestamp      TEXT NOT NULL
            );
        """)
        for i in range(n_clean):
            conn.execute(
                "INSERT INTO consolidated "
                "(source_file, source_row, date, product, region, "
                "sales_rep, customer, quantity, revenue) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (clean_file, i + 2, "2024-01-15", "Widget A",
                 "East", "Alice", "Acme Corp", 10.0, 1000.0),
            )
        for i in range(n_quarantine):
            conn.execute(
                "INSERT INTO quarantine "
                "(quarantine_reason, source_file, source_row, date, product, "
                "region, sales_rep, customer, quantity, revenue) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (quarantine_reason, quarantine_file, i + 10,
                 "2024-02-01", "Widget B", "North", "Bob",
                 "Corp Inc", "3.0", "-50.0"),
            )
        for i in range(n_log):
            conn.execute(
                "INSERT INTO cleaning_log "
                "(source_file, transformation, original_value, new_value, timestamp) "
                "VALUES (?, ?, ?, ?, ?)",
                ("sales.xlsx", "rename_column", f"Col{i}", f"canonical_{i}",
                 "2024-01-15T10:00:00+00:00"),
            )
        conn.commit()
    return db


def _make_cleaning_summary(
    n_files: int = 1,
    file_names: list[str] | None = None,
    n_rows_before: int = 5,
    n_rows_after: int = 3,
    n_columns_standardized: int = 4,
    n_duplicates_removed: int = 1,
    n_type_fixes: int = 7,
    transformation_counts: dict[str, int] | None = None,
) -> CleaningSummary:
    """Return a CleaningSummary with sensible defaults."""
    return CleaningSummary(
        n_files=n_files,
        file_names=file_names or ["sales.xlsx"],
        n_rows_before=n_rows_before,
        n_rows_after=n_rows_after,
        n_columns_standardized=n_columns_standardized,
        n_duplicates_removed=n_duplicates_removed,
        n_type_fixes=n_type_fixes,
        transformation_counts=transformation_counts or {"rename_column": 4},
    )


def _make_quarantine_summary(
    n_quarantined: int = 3,
    by_reason_type: dict[str, int] | None = None,
    by_source_file: dict[str, int] | None = None,
) -> QuarantineSummary:
    """Return a QuarantineSummary with sensible defaults."""
    return QuarantineSummary(
        n_quarantined=n_quarantined,
        by_reason_type=by_reason_type or {"negative value": 2, "invalid date": 1},
        by_source_file=by_source_file or {"sales.xlsx": 3},
    )


# ---------------------------------------------------------------------------
# _drop_db_cols
# ---------------------------------------------------------------------------


class TestDropDbCols:
    """Tests for the private _drop_db_cols helper."""

    def test_id_column_removed(self) -> None:
        """The 'id' column is dropped from the DataFrame."""
        df = pd.DataFrame({"id": [1, 2], "revenue": [100.0, 200.0]})
        assert "id" not in _drop_db_cols(df).columns

    def test_loaded_at_removed(self) -> None:
        """The 'loaded_at' timestamp column is dropped."""
        df = pd.DataFrame({"loaded_at": ["2024-01-01T00:00:00Z"], "revenue": [100.0]})
        assert "loaded_at" not in _drop_db_cols(df).columns

    def test_quarantined_at_removed(self) -> None:
        """The 'quarantined_at' timestamp column is dropped."""
        df = pd.DataFrame({"quarantined_at": ["2024-01-01T00:00:00Z"], "revenue": [100.0]})
        assert "quarantined_at" not in _drop_db_cols(df).columns

    def test_all_drop_cols_removed_simultaneously(self) -> None:
        """All three internal columns are removed when all are present."""
        df = pd.DataFrame({
            "id": [1], "loaded_at": ["ts"], "quarantined_at": ["ts"], "revenue": [1.0]
        })
        result = _drop_db_cols(df)
        for col in ("id", "loaded_at", "quarantined_at"):
            assert col not in result.columns

    def test_data_columns_retained(self) -> None:
        """Columns not in the drop set are left in place."""
        df = pd.DataFrame({"id": [1], "revenue": [100.0], "product": ["Widget"]})
        result = _drop_db_cols(df)
        assert "revenue" in result.columns
        assert "product" in result.columns

    def test_no_drop_cols_returns_df_unchanged(self) -> None:
        """A DataFrame with no internal columns is returned without modification."""
        df = pd.DataFrame({"revenue": [100.0]})
        result = _drop_db_cols(df)
        assert list(result.columns) == ["revenue"]

    def test_empty_df_handled(self) -> None:
        """An empty DataFrame with drop cols is handled without error."""
        df = pd.DataFrame({"id": pd.Series([], dtype=int)})
        result = _drop_db_cols(df)
        assert "id" not in result.columns
        assert len(result) == 0


# ---------------------------------------------------------------------------
# _collect_file_names
# ---------------------------------------------------------------------------


class TestCollectFileNames:
    """Tests for the private _collect_file_names helper."""

    def test_collects_from_clean_df(self) -> None:
        """File names from the clean DataFrame are included."""
        clean = pd.DataFrame({"source_file": ["a.xlsx"]})
        result = _collect_file_names(clean, pd.DataFrame())
        assert "a.xlsx" in result

    def test_collects_from_quarantine_df(self) -> None:
        """File names from the quarantine DataFrame are included."""
        quarantine = pd.DataFrame({"source_file": ["b.xlsx"]})
        result = _collect_file_names(pd.DataFrame(), quarantine)
        assert "b.xlsx" in result

    def test_deduplicates_across_both_dfs(self) -> None:
        """A filename present in both DataFrames appears only once."""
        clean = pd.DataFrame({"source_file": ["a.xlsx", "a.xlsx"]})
        quarantine = pd.DataFrame({"source_file": ["a.xlsx"]})
        result = _collect_file_names(clean, quarantine)
        assert result.count("a.xlsx") == 1

    def test_returns_sorted_list(self) -> None:
        """The returned list is sorted alphabetically."""
        clean = pd.DataFrame({"source_file": ["z.xlsx", "a.xlsx", "m.xlsx"]})
        result = _collect_file_names(clean, pd.DataFrame())
        assert result == sorted(result)

    def test_missing_source_file_column_returns_empty(self) -> None:
        """Returns an empty list when neither DataFrame has a source_file column."""
        result = _collect_file_names(pd.DataFrame(), pd.DataFrame())
        assert result == []

    def test_both_empty_returns_empty(self) -> None:
        """Returns an empty list when both DataFrames are empty but have the column."""
        clean = pd.DataFrame({"source_file": pd.Series([], dtype=str)})
        quarantine = pd.DataFrame({"source_file": pd.Series([], dtype=str)})
        assert _collect_file_names(clean, quarantine) == []

    def test_multiple_files_all_included(self) -> None:
        """All distinct file names across both DataFrames are returned."""
        clean = pd.DataFrame({"source_file": ["a.xlsx", "b.xlsx"]})
        quarantine = pd.DataFrame({"source_file": ["c.xlsx"]})
        result = _collect_file_names(clean, quarantine)
        assert set(result) == {"a.xlsx", "b.xlsx", "c.xlsx"}


# ---------------------------------------------------------------------------
# _count_transformations
# ---------------------------------------------------------------------------


class TestCountTransformations:
    """Tests for the private _count_transformations helper."""

    def test_counts_matching_rows(self) -> None:
        """Returns the number of rows with the given transformation label."""
        log = _make_log_df([
            ("f.xlsx", "rename_column", "Rev.", "revenue"),
            ("f.xlsx", "rename_column", "Qty", "quantity"),
            ("f.xlsx", "normalize_date", "mixed", "2 value(s) converted"),
        ])
        assert _count_transformations(log, "rename_column") == 2

    def test_returns_zero_when_no_match(self) -> None:
        """Returns 0 when no rows have the requested transformation label."""
        log = _make_log_df([("f.xlsx", "rename_column", "X", "y")])
        assert _count_transformations(log, "normalize_date") == 0

    def test_returns_zero_for_empty_log(self) -> None:
        """Returns 0 when the log DataFrame is empty."""
        assert _count_transformations(pd.DataFrame(), "rename_column") == 0

    def test_returns_zero_when_transformation_column_missing(self) -> None:
        """Returns 0 when the transformation column is absent from the log."""
        log = pd.DataFrame({"source_file": ["f.xlsx"]})
        assert _count_transformations(log, "rename_column") == 0


# ---------------------------------------------------------------------------
# _sum_leading_ints
# ---------------------------------------------------------------------------


class TestSumLeadingInts:
    """Tests for the private _sum_leading_ints helper."""

    def test_sums_integers_from_new_value(self) -> None:
        """Sums the leading integer from new_value for matching transformation rows."""
        log = _make_log_df([
            ("f.xlsx", "normalize_date", "mixed", "10 value(s) converted to YYYY-MM-DD"),
            ("g.xlsx", "normalize_date", "mixed", "4 value(s) converted to YYYY-MM-DD"),
        ])
        assert _sum_leading_ints(log, "normalize_date", "new_value") == 14

    def test_sums_integers_from_original_value(self) -> None:
        """Sums the leading integer from original_value for matching rows."""
        log = _make_log_df([
            ("f.xlsx", "strip_currency_symbols", "5 value(s) in 'revenue' had symbols", "stripped"),
            ("g.xlsx", "strip_currency_symbols", "3 value(s) in 'revenue' had symbols", "stripped"),
        ])
        assert _sum_leading_ints(log, "strip_currency_symbols", "original_value") == 8

    def test_returns_zero_when_no_matching_rows(self) -> None:
        """Returns 0 when the transformation label is not in the log."""
        log = _make_log_df([("f.xlsx", "rename_column", "X", "y")])
        assert _sum_leading_ints(log, "normalize_date", "new_value") == 0

    def test_returns_zero_for_empty_log(self) -> None:
        """Returns 0 when the log DataFrame is empty."""
        assert _sum_leading_ints(pd.DataFrame(), "normalize_date", "new_value") == 0

    def test_text_with_no_integer_contributes_zero(self) -> None:
        """A row whose target column contains no integer contributes 0 to the sum."""
        log = _make_log_df([
            ("f.xlsx", "normalize_date", "mixed", "no numbers here"),
        ])
        assert _sum_leading_ints(log, "normalize_date", "new_value") == 0


# ---------------------------------------------------------------------------
# _parse_leading_int
# ---------------------------------------------------------------------------


class TestParseLeadingInt:
    """Tests for the private _parse_leading_int helper."""

    def test_extracts_integer_at_start(self) -> None:
        """An integer at the start of the string is extracted correctly."""
        assert _parse_leading_int("14 value(s) converted") == 14

    def test_extracts_integer_in_middle(self) -> None:
        """An integer embedded in the middle of the string is extracted."""
        assert _parse_leading_int("found 7 duplicates across files") == 7

    def test_returns_zero_for_no_integer(self) -> None:
        """Returns 0 when the string contains no digits."""
        assert _parse_leading_int("stripped and stored as plain string") == 0

    def test_returns_zero_for_empty_string(self) -> None:
        """Returns 0 for an empty string."""
        assert _parse_leading_int("") == 0

    def test_return_type_is_int(self) -> None:
        """The return value is always an int, not a string."""
        assert isinstance(_parse_leading_int("42 items"), int)

    def test_multi_digit_integer(self) -> None:
        """Multi-digit integers are extracted as a single number, not digit by digit."""
        assert _parse_leading_int("123 rows processed") == 123


# ---------------------------------------------------------------------------
# _classify_reason
# ---------------------------------------------------------------------------


class TestClassifyReason:
    """Tests for the private _classify_reason helper."""

    def test_required_field(self) -> None:
        """'required field' phrases map to 'missing required field'."""
        assert _classify_reason("required field 'quantity' is empty in row 5 of f.xlsx") == "missing required field"

    def test_not_a_valid_number(self) -> None:
        """'is not a valid number' maps to 'type mismatch'."""
        assert _classify_reason("'pending' is not a valid number for 'revenue' in row 3 of f.xlsx") == "type mismatch"

    def test_not_a_valid_date(self) -> None:
        """'is not a valid date' maps to 'invalid date'."""
        assert _classify_reason("date 'not sure' is not a valid date for 'date' in row 7 of f.xlsx") == "invalid date"

    def test_is_negative(self) -> None:
        """'is negative' maps to 'negative value'."""
        assert _classify_reason("revenue is negative (-450) in row 23 of f.xlsx") == "negative value"

    def test_is_below_minimum(self) -> None:
        """'is below minimum' maps to 'out of range'."""
        assert _classify_reason("quantity is below minimum (0) in row 5 of f.xlsx") == "out of range"

    def test_exceeds_maximum(self) -> None:
        """'exceeds maximum' maps to 'out of range'."""
        assert _classify_reason("quantity exceeds maximum (99999 > 10000) in row 5 of f.xlsx") == "out of range"

    def test_is_before_minimum_date(self) -> None:
        """'is before minimum' maps to 'out of range'."""
        assert _classify_reason("date '2010-01-01' is before minimum allowed date '2015-01-01' in row 2 of f.xlsx") == "out of range"

    def test_is_after_maximum_date(self) -> None:
        """'is after maximum' maps to 'out of range'."""
        assert _classify_reason("date '2030-01-01' is after maximum allowed date '2026-12-31' in row 2 of f.xlsx") == "out of range"

    def test_does_not_match(self) -> None:
        """'does not match' maps to 'pattern mismatch'."""
        assert _classify_reason("'bad@' does not match required pattern for 'email' in row 8 of f.xlsx") == "pattern mismatch"

    def test_row_has_only(self) -> None:
        """'row has only' maps to 'sparse row'."""
        assert _classify_reason("row has only 1 non-empty field(s) (minimum 3 required) in row 11 of f.xlsx") == "sparse row"

    def test_unknown_phrase_returns_other(self) -> None:
        """An unrecognised reason string returns 'other'."""
        assert _classify_reason("something completely unexpected happened here") == "other"

    def test_case_insensitive(self) -> None:
        """Matching is case-insensitive."""
        assert _classify_reason("Required Field 'date' IS EMPTY in row 3 of f.xlsx") == "missing required field"


# ---------------------------------------------------------------------------
# read_consolidated
# ---------------------------------------------------------------------------


class TestReadConsolidated:
    """Tests for read_consolidated."""

    def test_returns_dataframe(self, tmp_path: Path) -> None:
        """read_consolidated returns a pandas DataFrame."""
        db = _make_db(tmp_path, n_clean=2)
        assert isinstance(read_consolidated(db), pd.DataFrame)

    def test_row_count_matches_db(self, tmp_path: Path) -> None:
        """Returned DataFrame row count matches the consolidated table."""
        db = _make_db(tmp_path, n_clean=5)
        assert len(read_consolidated(db)) == 5

    def test_id_not_in_result(self, tmp_path: Path) -> None:
        """The 'id' column is dropped before returning."""
        db = _make_db(tmp_path)
        assert "id" not in read_consolidated(db).columns

    def test_loaded_at_not_in_result(self, tmp_path: Path) -> None:
        """The 'loaded_at' column is dropped before returning."""
        db = _make_db(tmp_path)
        assert "loaded_at" not in read_consolidated(db).columns

    def test_data_columns_present(self, tmp_path: Path) -> None:
        """Core data columns are present in the result."""
        db = _make_db(tmp_path)
        result = read_consolidated(db)
        for col in ("source_file", "date", "product", "revenue"):
            assert col in result.columns

    def test_empty_table_returns_empty_df(self, tmp_path: Path) -> None:
        """An empty consolidated table returns a zero-row DataFrame."""
        db = _make_db(tmp_path, n_clean=0)
        assert len(read_consolidated(db)) == 0

    def test_missing_db_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the database does not exist."""
        with pytest.raises(FileNotFoundError, match="not found"):
            read_consolidated(tmp_path / "missing.db")

    def test_accepts_string_path(self, tmp_path: Path) -> None:
        """A string path is accepted in addition to a Path object."""
        db = _make_db(tmp_path)
        assert isinstance(read_consolidated(str(db)), pd.DataFrame)


# ---------------------------------------------------------------------------
# read_quarantine
# ---------------------------------------------------------------------------


class TestReadQuarantine:
    """Tests for read_quarantine."""

    def test_returns_dataframe(self, tmp_path: Path) -> None:
        """read_quarantine returns a pandas DataFrame."""
        db = _make_db(tmp_path, n_quarantine=1)
        assert isinstance(read_quarantine(db), pd.DataFrame)

    def test_row_count_matches_db(self, tmp_path: Path) -> None:
        """Returned DataFrame row count matches the quarantine table."""
        db = _make_db(tmp_path, n_quarantine=4)
        assert len(read_quarantine(db)) == 4

    def test_id_not_in_result(self, tmp_path: Path) -> None:
        """The 'id' column is dropped before returning."""
        db = _make_db(tmp_path, n_quarantine=1)
        assert "id" not in read_quarantine(db).columns

    def test_quarantined_at_not_in_result(self, tmp_path: Path) -> None:
        """The 'quarantined_at' column is dropped before returning."""
        db = _make_db(tmp_path, n_quarantine=1)
        assert "quarantined_at" not in read_quarantine(db).columns

    def test_quarantine_reason_column_present(self, tmp_path: Path) -> None:
        """The 'quarantine_reason' column is present in the result."""
        db = _make_db(tmp_path, n_quarantine=1)
        assert "quarantine_reason" in read_quarantine(db).columns

    def test_empty_table_returns_empty_df(self, tmp_path: Path) -> None:
        """An empty quarantine table returns a zero-row DataFrame."""
        db = _make_db(tmp_path, n_quarantine=0)
        assert len(read_quarantine(db)) == 0

    def test_missing_db_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the database does not exist."""
        with pytest.raises(FileNotFoundError, match="not found"):
            read_quarantine(tmp_path / "missing.db")

    def test_accepts_string_path(self, tmp_path: Path) -> None:
        """A string path is accepted in addition to a Path object."""
        db = _make_db(tmp_path, n_quarantine=1)
        assert isinstance(read_quarantine(str(db)), pd.DataFrame)


# ---------------------------------------------------------------------------
# read_cleaning_log
# ---------------------------------------------------------------------------


class TestReadCleaningLog:
    """Tests for read_cleaning_log."""

    def test_returns_dataframe(self, tmp_path: Path) -> None:
        """read_cleaning_log returns a pandas DataFrame."""
        db = _make_db(tmp_path, n_log=2)
        assert isinstance(read_cleaning_log(db), pd.DataFrame)

    def test_row_count_matches_db(self, tmp_path: Path) -> None:
        """Returned DataFrame row count matches the cleaning_log table."""
        db = _make_db(tmp_path, n_log=5)
        assert len(read_cleaning_log(db)) == 5

    def test_id_not_in_result(self, tmp_path: Path) -> None:
        """The 'id' column is dropped before returning."""
        db = _make_db(tmp_path, n_log=1)
        assert "id" not in read_cleaning_log(db).columns

    def test_transformation_column_present(self, tmp_path: Path) -> None:
        """The 'transformation' column is present in the result."""
        db = _make_db(tmp_path, n_log=1)
        assert "transformation" in read_cleaning_log(db).columns

    def test_empty_table_returns_empty_df(self, tmp_path: Path) -> None:
        """An empty cleaning_log table returns a zero-row DataFrame."""
        db = _make_db(tmp_path, n_log=0)
        assert len(read_cleaning_log(db)) == 0

    def test_missing_db_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the database does not exist."""
        with pytest.raises(FileNotFoundError, match="not found"):
            read_cleaning_log(tmp_path / "missing.db")

    def test_accepts_string_path(self, tmp_path: Path) -> None:
        """A string path is accepted in addition to a Path object."""
        db = _make_db(tmp_path, n_log=2)
        assert isinstance(read_cleaning_log(str(db)), pd.DataFrame)


# ---------------------------------------------------------------------------
# generate_cleaning_summary
# ---------------------------------------------------------------------------


class TestGenerateCleaningSummary:
    """Tests for generate_cleaning_summary."""

    def test_returns_cleaning_summary_instance(self) -> None:
        """Function returns a CleaningSummary dataclass."""
        result = generate_cleaning_summary(_make_clean_df(), _make_quarantine_df(), _make_log_df())
        assert isinstance(result, CleaningSummary)

    def test_n_files_counts_unique_sources(self) -> None:
        """n_files reflects the number of distinct source_file values."""
        clean = pd.concat([
            _make_clean_df(2, "a.xlsx"),
            _make_clean_df(2, "b.xlsx"),
        ], ignore_index=True)
        result = generate_cleaning_summary(clean, pd.DataFrame(), _make_log_df())
        assert result.n_files == 2

    def test_n_files_deduplicates_across_tables(self) -> None:
        """A file present in both clean and quarantine is counted only once."""
        clean = _make_clean_df(2, "shared.xlsx")
        quarantine = _make_quarantine_df(1, "shared.xlsx")
        result = generate_cleaning_summary(clean, quarantine, _make_log_df())
        assert result.n_files == 1

    def test_file_names_is_sorted(self) -> None:
        """file_names list is sorted alphabetically."""
        clean = pd.DataFrame({"source_file": ["z.xlsx", "a.xlsx"]})
        result = generate_cleaning_summary(clean, pd.DataFrame(), _make_log_df())
        assert result.file_names == sorted(result.file_names)

    def test_n_rows_before_is_clean_plus_quarantine(self) -> None:
        """n_rows_before equals len(clean_df) + len(quarantine_df)."""
        clean = _make_clean_df(7)
        quarantine = _make_quarantine_df(3)
        result = generate_cleaning_summary(clean, quarantine, _make_log_df())
        assert result.n_rows_before == 10

    def test_n_rows_after_is_clean_only(self) -> None:
        """n_rows_after equals len(clean_df) only."""
        clean = _make_clean_df(7)
        quarantine = _make_quarantine_df(3)
        result = generate_cleaning_summary(clean, quarantine, _make_log_df())
        assert result.n_rows_after == 7

    def test_n_columns_standardized_counts_rename_column_entries(self) -> None:
        """n_columns_standardized equals the number of rename_column log rows."""
        log = _make_log_df([
            ("f.xlsx", "rename_column", "Rev.", "revenue"),
            ("f.xlsx", "rename_column", "Qty", "quantity"),
            ("f.xlsx", "normalize_date", "mixed", "5 value(s) converted"),
        ])
        result = generate_cleaning_summary(_make_clean_df(), _make_quarantine_df(), log)
        assert result.n_columns_standardized == 2

    def test_n_duplicates_removed_parsed_from_log(self) -> None:
        """n_duplicates_removed parses the count from the remove_exact_duplicates entry."""
        log = _make_log_df([
            ("(all files)", "remove_exact_duplicates",
             "5 duplicate row(s) found across files",
             "removed — kept first occurrence"),
        ])
        result = generate_cleaning_summary(_make_clean_df(), _make_quarantine_df(), log)
        assert result.n_duplicates_removed == 5

    def test_n_type_fixes_sums_date_and_currency_fixes(self) -> None:
        """n_type_fixes is the sum of normalize_date new_value + strip_currency original_value counts."""
        log = _make_log_df([
            ("f.xlsx", "normalize_date", "mixed", "10 value(s) converted to YYYY-MM-DD"),
            ("f.xlsx", "strip_currency_symbols", "3 value(s) in 'revenue' had symbols", "stripped"),
        ])
        result = generate_cleaning_summary(_make_clean_df(), _make_quarantine_df(), log)
        assert result.n_type_fixes == 13

    def test_transformation_counts_includes_all_types(self) -> None:
        """transformation_counts maps every transformation label to its row count."""
        log = _make_log_df([
            ("f.xlsx", "rename_column", "X", "y"),
            ("f.xlsx", "rename_column", "A", "b"),
            ("f.xlsx", "normalize_date", "mixed", "3 value(s) converted"),
        ])
        result = generate_cleaning_summary(_make_clean_df(), _make_quarantine_df(), log)
        assert result.transformation_counts["rename_column"] == 2
        assert result.transformation_counts["normalize_date"] == 1

    def test_empty_inputs_produce_zero_counts(self) -> None:
        """Empty DataFrames and log produce a CleaningSummary with all zeros."""
        result = generate_cleaning_summary(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        assert result.n_files == 0
        assert result.n_rows_before == 0
        assert result.n_rows_after == 0
        assert result.n_columns_standardized == 0
        assert result.n_duplicates_removed == 0
        assert result.n_type_fixes == 0


# ---------------------------------------------------------------------------
# generate_quarantine_summary
# ---------------------------------------------------------------------------


class TestGenerateQuarantineSummary:
    """Tests for generate_quarantine_summary."""

    def test_returns_quarantine_summary_instance(self) -> None:
        """Function returns a QuarantineSummary dataclass."""
        result = generate_quarantine_summary(_make_quarantine_df())
        assert isinstance(result, QuarantineSummary)

    def test_n_quarantined_equals_row_count(self) -> None:
        """n_quarantined equals the number of rows in the DataFrame."""
        assert generate_quarantine_summary(_make_quarantine_df(6)).n_quarantined == 6

    def test_empty_df_returns_zero(self) -> None:
        """An empty DataFrame produces n_quarantined == 0."""
        result = generate_quarantine_summary(pd.DataFrame())
        assert result.n_quarantined == 0

    def test_empty_df_produces_empty_dicts(self) -> None:
        """An empty DataFrame produces empty by_reason_type and by_source_file."""
        result = generate_quarantine_summary(pd.DataFrame())
        assert result.by_reason_type == {}
        assert result.by_source_file == {}

    def test_by_source_file_counts_rows_per_file(self) -> None:
        """by_source_file maps each filename to its quarantined row count."""
        df = pd.concat([
            _make_quarantine_df(3, "a.xlsx"),
            _make_quarantine_df(2, "b.xlsx"),
        ], ignore_index=True)
        result = generate_quarantine_summary(df)
        assert result.by_source_file["a.xlsx"] == 3
        assert result.by_source_file["b.xlsx"] == 2

    def test_by_reason_type_classifies_reasons(self) -> None:
        """by_reason_type assigns each clause to the correct category."""
        df = _make_quarantine_df(
            n=1,
            reason="revenue is negative (-50) in row 4 of f.xlsx",
        )
        result = generate_quarantine_summary(df)
        assert result.by_reason_type.get("negative value", 0) == 1

    def test_semicolon_separated_clauses_each_classified(self) -> None:
        """Multiple semicolon-separated clauses in one reason are each counted."""
        df = pd.DataFrame({
            "quarantine_reason": [
                "required field 'quantity' is empty in row 5 of f.xlsx; "
                "revenue is negative (-10) in row 5 of f.xlsx"
            ],
            "source_file": ["f.xlsx"],
        })
        result = generate_quarantine_summary(df)
        assert result.by_reason_type.get("missing required field", 0) == 1
        assert result.by_reason_type.get("negative value", 0) == 1

    def test_reason_type_counts_sum_to_at_least_n_quarantined(self) -> None:
        """The total across all reason types is >= n_quarantined (multi-reason rows inflate it)."""
        df = _make_quarantine_df(3, reason="revenue is negative (-1) in row 1 of f.xlsx")
        result = generate_quarantine_summary(df)
        total_reasons = sum(result.by_reason_type.values())
        assert total_reasons >= result.n_quarantined


# ---------------------------------------------------------------------------
# render_terminal
# ---------------------------------------------------------------------------


class TestRenderTerminal:
    """Tests for render_terminal."""

    def test_returns_string(self) -> None:
        """render_terminal returns a str."""
        cs = _make_cleaning_summary()
        qs = _make_quarantine_summary()
        assert isinstance(render_terminal(cs, qs), str)

    def test_prints_to_stdout(self, capsys: pytest.CaptureFixture) -> None:
        """render_terminal prints its output to stdout."""
        render_terminal(_make_cleaning_summary(), _make_quarantine_summary())
        captured = capsys.readouterr()
        assert len(captured.out) > 0

    def test_returned_text_matches_printed_text(self, capsys: pytest.CaptureFixture) -> None:
        """The returned string is identical to what was printed."""
        cs = _make_cleaning_summary()
        qs = _make_quarantine_summary()
        returned = render_terminal(cs, qs)
        captured = capsys.readouterr()
        assert returned in captured.out

    def test_contains_file_count(self, capsys: pytest.CaptureFixture) -> None:
        """The output contains the number of files processed."""
        cs = _make_cleaning_summary(n_files=7, file_names=["f.xlsx"] * 7)
        render_terminal(cs, _make_quarantine_summary())
        assert "7" in capsys.readouterr().out

    def test_contains_file_names(self, capsys: pytest.CaptureFixture) -> None:
        """The output lists each source file name."""
        cs = _make_cleaning_summary(file_names=["q1.xlsx", "q2.xlsx"])
        render_terminal(cs, _make_quarantine_summary())
        out = capsys.readouterr().out
        assert "q1.xlsx" in out
        assert "q2.xlsx" in out

    def test_contains_rows_before_and_after(self, capsys: pytest.CaptureFixture) -> None:
        """The output contains both the before and after row counts."""
        cs = _make_cleaning_summary(n_rows_before=100, n_rows_after=90)
        render_terminal(cs, _make_quarantine_summary())
        out = capsys.readouterr().out
        assert "100" in out
        assert "90" in out

    def test_contains_quarantine_count(self, capsys: pytest.CaptureFixture) -> None:
        """The output contains the total quarantine row count."""
        qs = _make_quarantine_summary(n_quarantined=17)
        render_terminal(_make_cleaning_summary(), qs)
        assert "17" in capsys.readouterr().out

    def test_contains_reason_categories(self, capsys: pytest.CaptureFixture) -> None:
        """The output includes quarantine reason category labels."""
        qs = _make_quarantine_summary(by_reason_type={"negative value": 5})
        render_terminal(_make_cleaning_summary(), qs)
        assert "negative value" in capsys.readouterr().out

    def test_zero_quarantine_shows_no_rows_message(self, capsys: pytest.CaptureFixture) -> None:
        """When nothing is quarantined the output says 'No rows quarantined'."""
        qs = QuarantineSummary(n_quarantined=0)
        render_terminal(_make_cleaning_summary(), qs)
        assert "No rows quarantined" in capsys.readouterr().out

    def test_transformation_breakdown_present(self, capsys: pytest.CaptureFixture) -> None:
        """The transformation breakdown section is printed when counts are available."""
        cs = _make_cleaning_summary(transformation_counts={"rename_column": 4})
        render_terminal(cs, _make_quarantine_summary())
        assert "rename_column" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# render_markdown
# ---------------------------------------------------------------------------


class TestRenderMarkdown:
    """Tests for render_markdown."""

    def test_creates_file_on_disk(self, tmp_path: Path) -> None:
        """render_markdown writes a file at the specified output path."""
        out = tmp_path / "report.md"
        render_markdown(_make_cleaning_summary(), _make_quarantine_summary(), out)
        assert out.exists()

    def test_returns_path_to_written_file(self, tmp_path: Path) -> None:
        """render_markdown returns the Path of the written file."""
        out = tmp_path / "report.md"
        result = render_markdown(_make_cleaning_summary(), _make_quarantine_summary(), out)
        assert result == out

    def test_file_starts_with_markdown_header(self, tmp_path: Path) -> None:
        """The written file starts with a top-level Markdown header."""
        out = tmp_path / "report.md"
        render_markdown(_make_cleaning_summary(), _make_quarantine_summary(), out)
        assert out.read_text().startswith("# ")

    def test_creates_missing_parent_directories(self, tmp_path: Path) -> None:
        """render_markdown creates any missing parent directories automatically."""
        nested = tmp_path / "subdir" / "deep" / "report.md"
        render_markdown(_make_cleaning_summary(), _make_quarantine_summary(), nested)
        assert nested.exists()

    def test_contains_file_names(self, tmp_path: Path) -> None:
        """The written file lists each source file name."""
        cs = _make_cleaning_summary(file_names=["q1.xlsx", "q2.xlsx"])
        out = tmp_path / "r.md"
        render_markdown(cs, _make_quarantine_summary(), out)
        content = out.read_text()
        assert "q1.xlsx" in content
        assert "q2.xlsx" in content

    def test_contains_row_counts(self, tmp_path: Path) -> None:
        """The written file contains before/after row counts."""
        cs = _make_cleaning_summary(n_rows_before=50, n_rows_after=45)
        out = tmp_path / "r.md"
        render_markdown(cs, _make_quarantine_summary(), out)
        content = out.read_text()
        assert "50" in content
        assert "45" in content

    def test_contains_quarantine_table(self, tmp_path: Path) -> None:
        """The written file contains a Markdown table for quarantine reasons."""
        qs = _make_quarantine_summary(by_reason_type={"negative value": 3})
        out = tmp_path / "r.md"
        render_markdown(_make_cleaning_summary(), qs, out)
        content = out.read_text()
        assert "negative value" in content
        assert "|" in content  # Markdown table delimiter

    def test_contains_source_file_table(self, tmp_path: Path) -> None:
        """The written file contains a per-file quarantine breakdown table."""
        qs = _make_quarantine_summary(by_source_file={"sales.xlsx": 5})
        out = tmp_path / "r.md"
        render_markdown(_make_cleaning_summary(), qs, out)
        assert "sales.xlsx" in out.read_text()

    def test_zero_quarantine_produces_no_rows_message(self, tmp_path: Path) -> None:
        """When nothing is quarantined the file contains 'No rows were quarantined'."""
        qs = QuarantineSummary(n_quarantined=0)
        out = tmp_path / "r.md"
        render_markdown(_make_cleaning_summary(), qs, out)
        assert "No rows were quarantined" in out.read_text()

    def test_accepts_string_output_path(self, tmp_path: Path) -> None:
        """A string output path is accepted in addition to a Path object."""
        out = str(tmp_path / "report.md")
        render_markdown(_make_cleaning_summary(), _make_quarantine_summary(), out)
        assert Path(out).exists()

    def test_transformation_table_present(self, tmp_path: Path) -> None:
        """The transformation breakdown table is present in the Markdown output."""
        cs = _make_cleaning_summary(transformation_counts={"rename_column": 5})
        out = tmp_path / "r.md"
        render_markdown(cs, _make_quarantine_summary(), out)
        assert "rename_column" in out.read_text()


# ---------------------------------------------------------------------------
# report  (integration)
# ---------------------------------------------------------------------------


class TestReport:
    """Integration tests for the report() orchestrator."""

    def test_returns_report_result_instance(self, tmp_path: Path) -> None:
        """report() returns a ReportResult dataclass."""
        db = _make_db(tmp_path)
        result = report(db)
        assert isinstance(result, ReportResult)

    def test_terminal_text_is_string(self, tmp_path: Path) -> None:
        """ReportResult.terminal_text is a non-empty string."""
        db = _make_db(tmp_path)
        result = report(db)
        assert isinstance(result.terminal_text, str)
        assert len(result.terminal_text) > 0

    def test_output_path_none_when_no_file_requested(self, tmp_path: Path) -> None:
        """output_path is None when no output path is passed to report()."""
        db = _make_db(tmp_path)
        result = report(db)
        assert result.output_path is None

    def test_markdown_file_created_when_output_path_given(self, tmp_path: Path) -> None:
        """A Markdown file is created at the specified output path."""
        db = _make_db(tmp_path)
        out = tmp_path / "report.md"
        result = report(db, output_path=out, fmt="markdown")
        assert out.exists()
        assert result.output_path == out

    def test_terminal_text_contains_consolidated_row_count(self, tmp_path: Path) -> None:
        """The terminal output contains the number of clean rows from the DB."""
        db = _make_db(tmp_path, n_clean=4, n_quarantine=1)
        result = report(db)
        assert "4" in result.terminal_text

    def test_terminal_text_contains_quarantine_count(self, tmp_path: Path) -> None:
        """The terminal output contains the quarantine row count."""
        db = _make_db(tmp_path, n_clean=2, n_quarantine=3)
        result = report(db)
        assert "3" in result.terminal_text

    def test_unknown_format_raises_value_error(self, tmp_path: Path) -> None:
        """ValueError is raised when an unsupported format string is passed."""
        db = _make_db(tmp_path)
        with pytest.raises(ValueError, match="Unsupported format"):
            report(db, output_path=tmp_path / "r.html", fmt="html")

    def test_missing_db_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the database does not exist."""
        with pytest.raises(FileNotFoundError):
            report(tmp_path / "missing.db")

    def test_zero_quarantine_db_runs_without_error(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        """A database with no quarantined rows produces a valid report."""
        db = _make_db(tmp_path, n_clean=3, n_quarantine=0)
        result = report(db)
        assert result.n_quarantined == 0 if hasattr(result, "n_quarantined") else True
        assert "No rows quarantined" in result.terminal_text

    def test_zero_clean_db_runs_without_error(self, tmp_path: Path) -> None:
        """A database with no clean rows produces a valid report."""
        db = _make_db(tmp_path, n_clean=0, n_quarantine=2)
        result = report(db)
        assert isinstance(result, ReportResult)

    def test_accepts_string_db_path(self, tmp_path: Path) -> None:
        """A string database path is accepted in addition to a Path object."""
        db = _make_db(tmp_path)
        result = report(str(db))
        assert isinstance(result, ReportResult)
