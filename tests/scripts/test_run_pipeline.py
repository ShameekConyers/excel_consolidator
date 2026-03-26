"""Tests for scripts/run_pipeline.py.

Covers unit tests for resolve_input_folder and _build_arg_parser, and
integration tests for run_pipeline() using minimal CSV fixtures and a
temporary YAML config written to tmp_path.  No Drive credentials or network
access are required.
"""

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make scripts/ importable, then let run_pipeline.py add src/ itself.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from run_pipeline import (
    _build_arg_parser,
    resolve_input_folder,
    run_pipeline,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


_MINIMAL_CSV = """\
date,product,region,sales_rep,customer,quantity,revenue
2024-01-15,Widget A,East,Alice,Acme Corp,10,1000.00
2024-02-20,Widget B,West,Bob,Corp Inc,5,500.00
2024-03-10,Widget C,East,Carol,MegaCorp,3,300.00
"""

_MINIMAL_YAML = """\
columns:
  date:
    type: date
    min: "2015-01-01"
    max: "2026-12-31"
    required: true
  product:
    type: text
    required: true
  region:
    type: text
    required: true
  sales_rep:
    type: text
    required: true
  customer:
    type: text
    required: true
  quantity:
    type: numeric
    min: 1
    required: true
  revenue:
    type: numeric
    min: 0
    required: true
min_non_null_fields: 2
flag_non_conforming_types: true
"""


def _write_input_folder(tmp_path: Path) -> Path:
    """Create a minimal input folder with one clean CSV file.

    Args:
        tmp_path: pytest tmp_path directory.

    Returns:
        Path to the folder containing the CSV.
    """
    folder = tmp_path / "input"
    folder.mkdir()
    (folder / "sales.csv").write_text(_MINIMAL_CSV, encoding="utf-8")
    return folder


def _write_config(tmp_path: Path) -> Path:
    """Write a minimal validation_rules.yaml to tmp_path.

    Args:
        tmp_path: pytest tmp_path directory.

    Returns:
        Path to the written YAML file.
    """
    cfg = tmp_path / "validation_rules.yaml"
    cfg.write_text(_MINIMAL_YAML, encoding="utf-8")
    return cfg


# ---------------------------------------------------------------------------
# resolve_input_folder
# ---------------------------------------------------------------------------


class TestResolveInputFolder:
    """Tests for resolve_input_folder."""

    def test_local_explicit_path_returned(self, tmp_path: Path) -> None:
        """An explicit local input path is returned as a Path object."""
        folder = tmp_path / "data"
        folder.mkdir()
        result = resolve_input_folder("local", str(folder), None, tmp_path)
        assert result == folder

    def test_local_none_falls_back_to_sample_dir(self, tmp_path: Path) -> None:
        """Passing None for local_input returns the project default sample dir."""
        from run_pipeline import _DEFAULT_SAMPLE_DIR
        result = resolve_input_folder("local", None, None, tmp_path)
        assert result == _DEFAULT_SAMPLE_DIR

    def test_local_returns_path_object(self, tmp_path: Path) -> None:
        """The returned value is a Path, not a string."""
        folder = tmp_path / "data"
        folder.mkdir()
        result = resolve_input_folder("local", str(folder), None, tmp_path)
        assert isinstance(result, Path)

    def test_local_nonexistent_path_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the local path does not exist."""
        with pytest.raises(FileNotFoundError, match="not found"):
            resolve_input_folder("local", str(tmp_path / "missing"), None, tmp_path)

    def test_gdrive_without_folder_id_raises_value_error(self, tmp_path: Path) -> None:
        """ValueError is raised when gdrive source is used without a folder_id."""
        with pytest.raises(ValueError, match="folder-id"):
            resolve_input_folder("gdrive", None, None, tmp_path)

    def test_gdrive_without_folder_id_error_mentions_seed_drive(self, tmp_path: Path) -> None:
        """The ValueError message references seed_drive.py to guide the user."""
        with pytest.raises(ValueError, match="seed_drive"):
            resolve_input_folder("gdrive", None, None, tmp_path)

    def test_unknown_source_raises_value_error(self, tmp_path: Path) -> None:
        """ValueError is raised for an unrecognised source string."""
        with pytest.raises(ValueError, match="Unknown source"):
            resolve_input_folder("s3", None, None, tmp_path)

    def test_gdrive_with_folder_id_calls_download(self, tmp_path: Path) -> None:
        """When source is gdrive and folder_id is set, _download_from_drive is called."""
        with patch("run_pipeline._download_from_drive", return_value=tmp_path) as mock_dl:
            result = resolve_input_folder("gdrive", None, "folder_abc", tmp_path)
        mock_dl.assert_called_once_with("folder_abc", tmp_path)
        assert result == tmp_path


# ---------------------------------------------------------------------------
# run_pipeline  (integration)
# ---------------------------------------------------------------------------


class TestRunPipeline:
    """Integration tests for run_pipeline().

    Uses real CSV files written to tmp_path and a real (minimal) YAML config
    to exercise the full consolidate → validate → load chain.
    """

    def test_returns_tuple_of_two(self, tmp_path: Path) -> None:
        """run_pipeline returns a two-element tuple."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        result = run_pipeline(folder, db, "seed", cfg)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_first_element_is_string(self, tmp_path: Path) -> None:
        """The first return value (validation summary) is a string."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        validation_summary, _ = run_pipeline(folder, db, "seed", cfg)
        assert isinstance(validation_summary, str)

    def test_validation_summary_contains_row_counts(self, tmp_path: Path) -> None:
        """The validation summary mentions how many rows passed."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        validation_summary, _ = run_pipeline(folder, db, "seed", cfg)
        assert "rows" in validation_summary.lower()

    def test_second_element_is_load_result(self, tmp_path: Path) -> None:
        """The second return value is a LoadResult dataclass."""
        from db_loader import LoadResult
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        _, load_result = run_pipeline(folder, db, "seed", cfg)
        assert isinstance(load_result, LoadResult)

    def test_database_file_created(self, tmp_path: Path) -> None:
        """run_pipeline creates the SQLite database file on disk."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        run_pipeline(folder, db, "seed", cfg)
        assert db.exists()

    def test_consolidated_table_contains_rows(self, tmp_path: Path) -> None:
        """The consolidated table in the written DB contains at least one row."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        run_pipeline(folder, db, "seed", cfg)
        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        assert count > 0

    def test_load_result_n_consolidated_matches_db(self, tmp_path: Path) -> None:
        """LoadResult.n_consolidated matches the actual row count in the DB."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        _, load_result = run_pipeline(folder, db, "seed", cfg)
        with sqlite3.connect(db) as conn:
            db_count = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        assert load_result.n_consolidated == db_count

    def test_seed_mode_recreates_database(self, tmp_path: Path) -> None:
        """Seed mode always produces the same row count on repeated runs."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        run_pipeline(folder, db, "seed", cfg)
        _, first = run_pipeline(folder, db, "seed", cfg)
        _, second = run_pipeline(folder, db, "seed", cfg)
        assert first.n_consolidated == second.n_consolidated

    def test_full_mode_appends_rows(self, tmp_path: Path) -> None:
        """Full mode accumulates rows in the database across multiple runs."""
        folder = _write_input_folder(tmp_path)
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        run_pipeline(folder, db, "seed", cfg)  # create initial DB
        run_pipeline(folder, db, "full", cfg)
        with sqlite3.connect(db) as conn:
            after_one = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        run_pipeline(folder, db, "full", cfg)
        with sqlite3.connect(db) as conn:
            after_two = conn.execute("SELECT COUNT(*) FROM consolidated").fetchone()[0]
        assert after_two > after_one

    def test_missing_input_folder_raises_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError propagates when the input folder does not exist."""
        cfg = _write_config(tmp_path)
        db = tmp_path / "out.db"
        with pytest.raises(FileNotFoundError):
            run_pipeline(tmp_path / "missing", db, "seed", cfg)


# ---------------------------------------------------------------------------
# _build_arg_parser
# ---------------------------------------------------------------------------


class TestBuildArgParser:
    """Tests for _build_arg_parser — argument defaults and accepted values."""

    def _parse(self, *args: str) -> object:
        """Parse a list of CLI argument strings and return the namespace."""
        return _build_arg_parser().parse_args(list(args))

    def test_default_mode_is_seed(self) -> None:
        """The --mode flag defaults to 'seed' when not supplied."""
        assert self._parse().mode == "seed"

    def test_default_source_is_local(self) -> None:
        """The --source flag defaults to 'local' when not supplied."""
        assert self._parse().source == "local"

    def test_default_input_is_none(self) -> None:
        """The --input flag defaults to None when not supplied."""
        assert self._parse().input is None

    def test_default_db_is_none(self) -> None:
        """The --db flag defaults to None when not supplied."""
        assert self._parse().db is None

    def test_mode_full_accepted(self) -> None:
        """--mode full is accepted and stored on the namespace."""
        assert self._parse("--mode", "full").mode == "full"

    def test_source_gdrive_accepted(self) -> None:
        """--source gdrive is accepted and stored on the namespace."""
        assert self._parse("--source", "gdrive").source == "gdrive"

    def test_input_flag_sets_value(self) -> None:
        """--input stores the supplied path string."""
        assert self._parse("--input", "/data/files").input == "/data/files"

    def test_folder_id_flag_sets_value(self) -> None:
        """--folder-id stores the supplied Drive folder ID."""
        assert self._parse("--folder-id", "abc123").folder_id == "abc123"

    def test_db_flag_sets_value(self) -> None:
        """--db stores the supplied database path string."""
        assert self._parse("--db", "data/my.db").db == "data/my.db"

    def test_config_flag_sets_value(self) -> None:
        """--config stores the supplied config path string."""
        assert self._parse("--config", "my_rules.yaml").config == "my_rules.yaml"

    def test_invalid_mode_raises_system_exit(self) -> None:
        """An unrecognised --mode value causes argparse to exit."""
        with pytest.raises(SystemExit):
            self._parse("--mode", "invalid")

    def test_invalid_source_raises_system_exit(self) -> None:
        """An unrecognised --source value causes argparse to exit."""
        with pytest.raises(SystemExit):
            self._parse("--source", "s3")
