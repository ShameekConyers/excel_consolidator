"""Tests for src/db_loader.py.

Covers unit tests for every public function and selected private helpers.
Integration-level tests use tmp_path SQLite databases to exercise load()
end-to-end, including the seed-vs-full mode difference.
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from consolidator import CleaningEntry
from db_loader import (
    LoadResult,
    _CONSOLIDATED_COLS,
    _LOG_COLS,
    _QUARANTINE_COLS,
    _cleaning_log_to_df,
    _prepare_consolidated,
    _prepare_quarantine,
    build_summary,
    init_schema,
    load,
    resolve_db_path,
    write_cleaning_log,
    write_consolidated,
    write_quarantine,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the schema already initialised."""
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    return conn


def _make_clean_df(n: int = 1) -> pd.DataFrame:
    """Return a minimal clean DataFrame with n rows."""
    return pd.DataFrame({
        "source_file": ["sales.xlsx"] * n,
        "source_row":  list(range(2, n + 2)),
        "date":        ["2024-01-15"] * n,
        "product":     ["Widget A"] * n,
        "region":      ["East"] * n,
        "sales_rep":   ["Alice"] * n,
        "customer":    ["Acme Corp"] * n,
        "quantity":    ["10.0"] * n,
        "revenue":     ["1000.0"] * n,
    })


def _make_quarantine_df(n: int = 1) -> pd.DataFrame:
    """Return a minimal quarantine DataFrame with n rows."""
    return pd.DataFrame({
        "quarantine_reason": ["revenue is negative (-50) in row 4 of sales.xlsx"] * n,
        "source_file": ["sales.xlsx"] * n,
        "source_row":  list(range(4, n + 4)),
        "date":        ["2024-03-01"] * n,
        "product":     ["Widget C"] * n,
        "region":      ["North"] * n,
        "sales_rep":   ["Carol"] * n,
        "customer":    ["Initech"] * n,
        "quantity":    ["3.0"] * n,
        "revenue":     ["-50.0"] * n,
    })


def _make_cleaning_log(n: int = 2) -> list[CleaningEntry]:
    """Return a list of up to 2 CleaningEntry records."""
    entries = [
        CleaningEntry("sales.xlsx", "rename_column", "Rev.", "revenue"),
        CleaningEntry("(all files)", "remove_exact_duplicates", "1 row", "removed"),
    ]
    return entries[:n]


# ---------------------------------------------------------------------------
# resolve_db_path
# ---------------------------------------------------------------------------


class TestResolveDbPath:
    """Tests for resolve_db_path."""

    def test_seed_mode_returns_data_seed_db(self) -> None:
        """seed mode returns <base_dir>/data/seed.db."""
        result = resolve_db_path("/projects/excel_consolidator", "seed")
        assert result == Path("/projects/excel_consolidator/data/seed.db")

    def test_full_mode_returns_data_output_full_db(self) -> None:
        """full mode returns <base_dir>/data/output/full.db."""
        result = resolve_db_path("/projects/excel_consolidator", "full")
        assert result == Path("/projects/excel_consolidator/data/output/full.db")

    def test_returns_path_object(self) -> None:
        """Return type is always a Path, not a string."""
        assert isinstance(resolve_db_path("/base", "seed"), Path)

    def test_invalid_mode_raises_value_error(self) -> None:
        """An unrecognised mode string raises ValueError."""
        with pytest.raises(ValueError, match="Unknown mode"):
            resolve_db_path("/base", "live")


# ---------------------------------------------------------------------------
# init_schema
# ---------------------------------------------------------------------------


class TestInitSchema:
    """Tests for init_schema."""

    def test_creates_consolidated_table(self) -> None:
        """consolidated table exists after init_schema."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "consolidated" in tables

    def test_creates_quarantine_table(self) -> None:
        """quarantine table exists after init_schema."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "quarantine" in tables

    def test_creates_cleaning_log_table(self) -> None:
        """cleaning_log table exists after init_schema."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "cleaning_log" in tables

    def test_consolidated_has_expected_columns(self) -> None:
        """consolidated table includes all canonical data columns plus id and loaded_at."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(consolidated)").fetchall()}
        for expected in ("id", "source_file", "source_row", "date", "product",
                         "region", "sales_rep", "customer", "quantity", "revenue", "loaded_at"):
            assert expected in cols, f"Missing column: {expected}"

    def test_quarantine_has_reason_column(self) -> None:
        """quarantine table includes the quarantine_reason column."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(quarantine)").fetchall()}
        assert "quarantine_reason" in cols

    def test_cleaning_log_has_expected_columns(self) -> None:
        """cleaning_log table includes transformation, original_value, new_value, timestamp."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(cleaning_log)").fetchall()}
        for expected in ("transformation", "original_value", "new_value", "timestamp"):
            assert expected in cols, f"Missing column: {expected}"

    def test_idempotent_when_called_twice(self) -> None:
        """Calling init_schema twice does not raise — IF NOT EXISTS is respected."""
        conn = sqlite3.connect(":memory:")
        init_schema(conn)
        init_schema(conn)  # should not raise


# ---------------------------------------------------------------------------
# write_consolidated
# ---------------------------------------------------------------------------


class TestWriteConsolidated:
    """Tests for write_consolidated."""

    def test_returns_row_count(self) -> None:
        """Returns the number of rows inserted."""
        assert write_consolidated(_conn(), _make_clean_df(3)) == 3

    def test_rows_present_in_db(self) -> None:
        """Inserted rows are retrievable from the consolidated table."""
        conn = _conn()
        write_consolidated(conn, _make_clean_df(2))
        count = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        assert count == 2

    def test_empty_df_returns_zero_and_inserts_nothing(self) -> None:
        """An empty DataFrame inserts nothing and returns 0."""
        conn = _conn()
        assert write_consolidated(conn, pd.DataFrame()) == 0
        assert conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0] == 0

    def test_extra_columns_in_df_are_ignored(self) -> None:
        """Columns not in the consolidated schema are silently dropped."""
        df = _make_clean_df()
        df["extra_field"] = "should_not_appear"
        conn = _conn()
        write_consolidated(conn, df)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(consolidated)").fetchall()}
        assert "extra_field" not in cols

    def test_missing_canonical_column_stored_as_null(self) -> None:
        """An absent canonical column (e.g. region) is stored as NULL."""
        df = _make_clean_df().drop(columns=["region"])
        conn = _conn()
        write_consolidated(conn, df)
        val = conn.execute("SELECT region FROM consolidated").fetchone()[0]
        assert val is None

    def test_quantity_stored_as_real(self) -> None:
        """quantity is coerced to float and stored as REAL, not TEXT."""
        conn = _conn()
        write_consolidated(conn, _make_clean_df())
        val = conn.execute("SELECT quantity FROM consolidated").fetchone()[0]
        assert isinstance(val, float)
        assert val == 10.0

    def test_revenue_stored_as_real(self) -> None:
        """revenue is coerced to float and stored as REAL, not TEXT."""
        conn = _conn()
        write_consolidated(conn, _make_clean_df())
        val = conn.execute("SELECT revenue FROM consolidated").fetchone()[0]
        assert isinstance(val, float)
        assert val == 1000.0

    def test_non_coercible_numeric_stored_as_null(self) -> None:
        """A non-numeric revenue string is coerced to NULL via errors='coerce'."""
        df = _make_clean_df()
        df["revenue"] = "not_a_number"
        conn = _conn()
        write_consolidated(conn, df)
        val = conn.execute("SELECT revenue FROM consolidated").fetchone()[0]
        assert val is None

    def test_loaded_at_auto_populated_by_sqlite_default(self) -> None:
        """loaded_at is set by SQLite's DEFAULT and is non-null after insert."""
        conn = _conn()
        write_consolidated(conn, _make_clean_df())
        val = conn.execute("SELECT loaded_at FROM consolidated").fetchone()[0]
        assert val is not None and val != ""


# ---------------------------------------------------------------------------
# write_quarantine
# ---------------------------------------------------------------------------


class TestWriteQuarantine:
    """Tests for write_quarantine."""

    def test_returns_row_count(self) -> None:
        """Returns the number of rows inserted."""
        assert write_quarantine(_conn(), _make_quarantine_df(2)) == 2

    def test_rows_present_in_db(self) -> None:
        """Inserted rows are retrievable from the quarantine table."""
        conn = _conn()
        write_quarantine(conn, _make_quarantine_df(3))
        count = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
        assert count == 3

    def test_empty_df_returns_zero_and_inserts_nothing(self) -> None:
        """An empty DataFrame inserts nothing and returns 0."""
        conn = _conn()
        assert write_quarantine(conn, pd.DataFrame()) == 0
        assert conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 0

    def test_quarantine_reason_preserved_verbatim(self) -> None:
        """The quarantine_reason string is stored exactly as supplied."""
        df = _make_quarantine_df()
        expected = df["quarantine_reason"].iloc[0]
        conn = _conn()
        write_quarantine(conn, df)
        stored = conn.execute("SELECT quarantine_reason FROM quarantine").fetchone()[0]
        assert stored == expected

    def test_invalid_revenue_string_kept_as_text(self) -> None:
        """An invalid revenue value (e.g. 'pending') is stored verbatim as TEXT."""
        df = _make_quarantine_df()
        df["revenue"] = "pending"
        conn = _conn()
        write_quarantine(conn, df)
        val = conn.execute("SELECT revenue FROM quarantine").fetchone()[0]
        assert val == "pending"

    def test_quarantined_at_auto_populated_by_sqlite_default(self) -> None:
        """quarantined_at is set by SQLite's DEFAULT and is non-null after insert."""
        conn = _conn()
        write_quarantine(conn, _make_quarantine_df())
        val = conn.execute("SELECT quarantined_at FROM quarantine").fetchone()[0]
        assert val is not None and val != ""


# ---------------------------------------------------------------------------
# write_cleaning_log
# ---------------------------------------------------------------------------


class TestWriteCleaningLog:
    """Tests for write_cleaning_log."""

    def test_returns_entry_count(self) -> None:
        """Returns the number of entries inserted."""
        assert write_cleaning_log(_conn(), _make_cleaning_log(2)) == 2

    def test_entries_present_in_db(self) -> None:
        """Inserted entries are retrievable from the cleaning_log table."""
        conn = _conn()
        write_cleaning_log(conn, _make_cleaning_log(2))
        count = conn.execute("SELECT COUNT(*) FROM cleaning_log").fetchone()[0]
        assert count == 2

    def test_empty_list_returns_zero_and_inserts_nothing(self) -> None:
        """An empty list inserts nothing and returns 0."""
        conn = _conn()
        assert write_cleaning_log(conn, []) == 0
        assert conn.execute("SELECT COUNT(*) FROM cleaning_log").fetchone()[0] == 0

    def test_all_fields_stored_correctly(self) -> None:
        """Every CleaningEntry field is stored with the correct value."""
        entry = CleaningEntry("sales.xlsx", "rename_column", "Rev.", "revenue")
        conn = _conn()
        write_cleaning_log(conn, [entry])
        row = conn.execute(
            "SELECT source_file, transformation, original_value, new_value, timestamp "
            "FROM cleaning_log"
        ).fetchone()
        assert row[0] == "sales.xlsx"
        assert row[1] == "rename_column"
        assert row[2] == "Rev."
        assert row[3] == "revenue"
        assert row[4] == entry.timestamp


# ---------------------------------------------------------------------------
# _prepare_consolidated  (private helper tested directly)
# ---------------------------------------------------------------------------


class TestPrepareConsolidated:
    """Tests for the private _prepare_consolidated helper."""

    def test_output_contains_exactly_consolidated_columns(self) -> None:
        """Output columns match _CONSOLIDATED_COLS exactly, in order."""
        df = _make_clean_df()
        df["extra"] = "extra"
        result = _prepare_consolidated(df)
        assert list(result.columns) == list(_CONSOLIDATED_COLS)

    def test_missing_canonical_column_filled_with_nan(self) -> None:
        """A missing canonical column becomes NaN in the output."""
        df = _make_clean_df().drop(columns=["region"])
        result = _prepare_consolidated(df)
        assert result["region"].isna().all()

    def test_quantity_coerced_to_float(self) -> None:
        """quantity column is cast to float dtype."""
        result = _prepare_consolidated(_make_clean_df())
        assert result["quantity"].dtype == float

    def test_revenue_coerced_to_float(self) -> None:
        """revenue column is cast to float dtype."""
        result = _prepare_consolidated(_make_clean_df())
        assert result["revenue"].dtype == float

    def test_non_numeric_revenue_becomes_nan(self) -> None:
        """A non-numeric revenue string becomes NaN after coercion."""
        df = _make_clean_df()
        df["revenue"] = "TBD"
        result = _prepare_consolidated(df)
        assert pd.isna(result["revenue"].iloc[0])

    def test_input_df_not_mutated(self) -> None:
        """The original DataFrame's column dtypes are not modified in place."""
        df = _make_clean_df()
        original_dtype = df["revenue"].dtype
        _prepare_consolidated(df)
        assert df["revenue"].dtype == original_dtype


# ---------------------------------------------------------------------------
# _prepare_quarantine  (private helper tested directly)
# ---------------------------------------------------------------------------


class TestPrepareQuarantine:
    """Tests for the private _prepare_quarantine helper."""

    def test_output_contains_exactly_quarantine_columns(self) -> None:
        """Output columns match _QUARANTINE_COLS exactly, in order."""
        result = _prepare_quarantine(_make_quarantine_df())
        assert list(result.columns) == list(_QUARANTINE_COLS)

    def test_quarantine_reason_is_first_column(self) -> None:
        """quarantine_reason is the leading column in the output."""
        result = _prepare_quarantine(_make_quarantine_df())
        assert result.columns[0] == "quarantine_reason"

    def test_numeric_columns_not_coerced_to_float(self) -> None:
        """quantity and revenue are not coerced to float — values remain as strings."""
        result = _prepare_quarantine(_make_quarantine_df())
        assert result["revenue"].dtype != float
        assert result["quantity"].dtype != float
        # Values must still be readable as their original strings
        assert result["revenue"].iloc[0] == "-50.0"
        assert result["quantity"].iloc[0] == "3.0"

    def test_extra_columns_are_dropped(self) -> None:
        """Columns not in the quarantine schema are removed from the output."""
        df = _make_quarantine_df()
        df["unrelated"] = "extra"
        result = _prepare_quarantine(df)
        assert "unrelated" not in result.columns


# ---------------------------------------------------------------------------
# _cleaning_log_to_df  (private helper tested directly)
# ---------------------------------------------------------------------------


class TestCleaningLogToDf:
    """Tests for the private _cleaning_log_to_df helper."""

    def test_returns_dataframe(self) -> None:
        """Output is a pandas DataFrame."""
        assert isinstance(_cleaning_log_to_df(_make_cleaning_log()), pd.DataFrame)

    def test_columns_match_log_schema(self) -> None:
        """Output columns match _LOG_COLS exactly."""
        assert list(_cleaning_log_to_df(_make_cleaning_log()).columns) == list(_LOG_COLS)

    def test_row_count_matches_input_length(self) -> None:
        """Number of rows equals number of CleaningEntry inputs."""
        assert len(_cleaning_log_to_df(_make_cleaning_log(2))) == 2

    def test_all_field_values_preserved(self) -> None:
        """Every CleaningEntry attribute appears correctly in the output DataFrame."""
        entry = CleaningEntry("f.csv", "rename_column", "Rev.", "revenue")
        result = _cleaning_log_to_df([entry])
        assert result["source_file"].iloc[0] == "f.csv"
        assert result["transformation"].iloc[0] == "rename_column"
        assert result["original_value"].iloc[0] == "Rev."
        assert result["new_value"].iloc[0] == "revenue"
        assert result["timestamp"].iloc[0] == entry.timestamp

    def test_empty_list_returns_empty_df_with_correct_columns(self) -> None:
        """An empty list produces a zero-row DataFrame with the log schema columns."""
        result = _cleaning_log_to_df([])
        assert len(result) == 0
        assert list(result.columns) == list(_LOG_COLS)


# ---------------------------------------------------------------------------
# build_summary
# ---------------------------------------------------------------------------


class TestBuildSummary:
    """Tests for build_summary."""

    def _result(
        self,
        n_consolidated: int = 10,
        n_quarantine: int = 3,
        n_log: int = 15,
    ) -> LoadResult:
        """Return a LoadResult with the given counts."""
        return LoadResult(
            db_path=Path("data/seed.db"),
            n_consolidated=n_consolidated,
            n_quarantine=n_quarantine,
            n_log_entries=n_log,
        )

    def test_contains_db_path(self) -> None:
        """Summary string includes the database file path."""
        assert "data/seed.db" in build_summary(self._result())

    def test_contains_consolidated_count(self) -> None:
        """Summary includes the number of consolidated rows."""
        assert "42" in build_summary(self._result(n_consolidated=42))

    def test_contains_quarantine_count(self) -> None:
        """Summary includes the number of quarantined rows."""
        assert "7" in build_summary(self._result(n_quarantine=7))

    def test_contains_log_count(self) -> None:
        """Summary includes the number of cleaning log entries."""
        assert "99" in build_summary(self._result(n_log=99))

    def test_zero_counts_are_shown_not_omitted(self) -> None:
        """Counts of zero are explicitly present in the summary."""
        msg = build_summary(self._result(n_consolidated=0, n_quarantine=0, n_log=0))
        assert "0" in msg


# ---------------------------------------------------------------------------
# load  (integration)
# ---------------------------------------------------------------------------


class TestLoad:
    """Integration tests for load using temporary SQLite databases."""

    def test_returns_load_result_instance(self, tmp_path: Path) -> None:
        """load returns a LoadResult."""
        result = load(
            _make_clean_df(), _make_quarantine_df(), _make_cleaning_log(),
            tmp_path / "test.db",
        )
        assert isinstance(result, LoadResult)

    def test_result_counts_match_input_sizes(self, tmp_path: Path) -> None:
        """LoadResult counts reflect the actual row/entry counts supplied."""
        result = load(
            _make_clean_df(3), _make_quarantine_df(2), _make_cleaning_log(2),
            tmp_path / "test.db",
        )
        assert result.n_consolidated == 3
        assert result.n_quarantine == 2
        assert result.n_log_entries == 2

    def test_result_db_path_matches_argument(self, tmp_path: Path) -> None:
        """LoadResult.db_path equals the path passed to load."""
        db = tmp_path / "output.db"
        assert load(_make_clean_df(), _make_quarantine_df(), _make_cleaning_log(), db).db_path == db

    def test_db_file_is_created_on_disk(self, tmp_path: Path) -> None:
        """The SQLite file is written to disk after load completes."""
        db = tmp_path / "test.db"
        load(_make_clean_df(), pd.DataFrame(), [], db)
        assert db.exists()

    def test_all_three_tables_populated_correctly(self, tmp_path: Path) -> None:
        """Each of the three tables contains the expected number of rows."""
        db = tmp_path / "test.db"
        load(_make_clean_df(5), _make_quarantine_df(2), _make_cleaning_log(2), db)
        conn = sqlite3.connect(db)
        assert conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM cleaning_log").fetchone()[0] == 2
        conn.close()

    def test_seed_mode_replaces_existing_rows(self, tmp_path: Path) -> None:
        """A second seed-mode load wipes previous data rather than appending."""
        db = tmp_path / "test.db"
        load(_make_clean_df(5), _make_quarantine_df(1), _make_cleaning_log(2), db, mode="seed")
        load(_make_clean_df(2), _make_quarantine_df(1), _make_cleaning_log(1), db, mode="seed")
        conn = sqlite3.connect(db)
        count = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        conn.close()
        assert count == 2  # not 7

    def test_full_mode_appends_to_existing_rows(self, tmp_path: Path) -> None:
        """A second full-mode load appends rows without replacing existing ones."""
        db = tmp_path / "test.db"
        load(_make_clean_df(3), pd.DataFrame(), [], db, mode="full")
        load(_make_clean_df(2), pd.DataFrame(), [], db, mode="full")
        conn = sqlite3.connect(db)
        count = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        conn.close()
        assert count == 5

    def test_invalid_mode_raises_value_error(self, tmp_path: Path) -> None:
        """An unrecognised mode string raises ValueError before writing anything."""
        with pytest.raises(ValueError, match="Unknown mode"):
            load(_make_clean_df(), _make_quarantine_df(), [], tmp_path / "x.db", mode="live")

    def test_creates_missing_parent_directories(self, tmp_path: Path) -> None:
        """load creates any missing parent directories for the database path."""
        nested_db = tmp_path / "a" / "b" / "c" / "test.db"
        load(_make_clean_df(), pd.DataFrame(), [], nested_db)
        assert nested_db.exists()

    def test_all_empty_inputs_succeed_with_zero_counts(self, tmp_path: Path) -> None:
        """load with empty DataFrames and an empty log completes without error."""
        result = load(pd.DataFrame(), pd.DataFrame(), [], tmp_path / "test.db")
        assert result.n_consolidated == 0
        assert result.n_quarantine == 0
        assert result.n_log_entries == 0
