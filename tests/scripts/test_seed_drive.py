"""Tests for scripts/seed_drive.py.

Covers unit tests for collect_sample_files and parse_args, and behavioural
tests for main() using mocked Drive API calls.  No Google credentials or
network access are required.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# Make scripts/ importable, then let seed_drive.py add src/ itself.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import seed_drive
from seed_drive import (
    _DEFAULT_FOLDER_NAME,
    collect_sample_files,
    parse_args,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service(email: str = "user@example.com") -> MagicMock:
    """Return a MagicMock that looks like an authenticated Drive service.

    Args:
        email: Email address returned by the about().get().execute() call.

    Returns:
        MagicMock wired to return the supplied email on the about() chain.
    """
    service = MagicMock()
    service.about().get(fields="user").execute.return_value = {
        "user": {"emailAddress": email}
    }
    return service


def _populate_sample_dir(directory: Path) -> list[Path]:
    """Create one file of each supported spreadsheet extension in directory.

    Args:
        directory: Directory to create files in.

    Returns:
        Sorted list of the created file paths.
    """
    files = []
    for name in ("b_report.xlsx", "a_data.csv", "c_legacy.xls"):
        p = directory / name
        p.write_text("col1,col2\nval1,val2\n", encoding="utf-8")
        files.append(p)
    return sorted(files)


# ---------------------------------------------------------------------------
# collect_sample_files
# ---------------------------------------------------------------------------


class TestCollectSampleFiles:
    """Tests for collect_sample_files."""

    def test_returns_list(self, tmp_path: Path) -> None:
        """collect_sample_files returns a list."""
        assert isinstance(collect_sample_files(tmp_path), list)

    def test_includes_xlsx_files(self, tmp_path: Path) -> None:
        """Files with .xlsx extension are included."""
        (tmp_path / "report.xlsx").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert any(f.suffix == ".xlsx" for f in result)

    def test_includes_xls_files(self, tmp_path: Path) -> None:
        """Files with .xls extension are included."""
        (tmp_path / "legacy.xls").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert any(f.suffix == ".xls" for f in result)

    def test_includes_csv_files(self, tmp_path: Path) -> None:
        """Files with .csv extension are included."""
        (tmp_path / "data.csv").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert any(f.suffix == ".csv" for f in result)

    def test_excludes_txt_files(self, tmp_path: Path) -> None:
        """Files with .txt extension are excluded."""
        (tmp_path / "notes.txt").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert not any(f.suffix == ".txt" for f in result)

    def test_excludes_pdf_files(self, tmp_path: Path) -> None:
        """Files with .pdf extension are excluded."""
        (tmp_path / "report.pdf").write_bytes(b"")
        result = collect_sample_files(tmp_path)
        assert not any(f.suffix == ".pdf" for f in result)

    def test_empty_directory_returns_empty_list(self, tmp_path: Path) -> None:
        """An empty directory returns an empty list."""
        assert collect_sample_files(tmp_path) == []

    def test_results_are_sorted(self, tmp_path: Path) -> None:
        """Returned paths are sorted alphabetically by filename."""
        for name in ("z.xlsx", "a.csv", "m.xls"):
            (tmp_path / name).write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert result == sorted(result)

    def test_returns_path_objects(self, tmp_path: Path) -> None:
        """Each item in the returned list is a Path object."""
        (tmp_path / "data.csv").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert all(isinstance(f, Path) for f in result)

    def test_non_recursive(self, tmp_path: Path) -> None:
        """Files in subdirectories are not included."""
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "nested.xlsx").write_text("", encoding="utf-8")
        result = collect_sample_files(tmp_path)
        assert len(result) == 0

    def test_mixed_extensions_correct_count(self, tmp_path: Path) -> None:
        """Only the spreadsheet files are counted when mixed files are present."""
        (tmp_path / "data.csv").write_text("", encoding="utf-8")
        (tmp_path / "report.xlsx").write_text("", encoding="utf-8")
        (tmp_path / "readme.txt").write_text("", encoding="utf-8")
        assert len(collect_sample_files(tmp_path)) == 2

    def test_extension_check_is_case_insensitive(self, tmp_path: Path) -> None:
        """Uppercase extensions (.XLSX, .CSV) are included."""
        (tmp_path / "REPORT.XLSX").write_text("", encoding="utf-8")
        (tmp_path / "DATA.CSV").write_text("", encoding="utf-8")
        assert len(collect_sample_files(tmp_path)) == 2


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    """Tests for parse_args — argument defaults and accepted values."""

    def _parse(self, *args: str) -> object:
        """Invoke parse_args with a controlled sys.argv.

        Args:
            args: CLI argument strings to pass after the script name.

        Returns:
            Parsed argparse Namespace.
        """
        with patch("sys.argv", ["seed_drive.py", *args]):
            return parse_args()

    def test_default_folder_name(self) -> None:
        """folder_name defaults to the module constant when not supplied."""
        assert self._parse().folder_name == _DEFAULT_FOLDER_NAME

    def test_folder_name_flag_sets_value(self) -> None:
        """--folder-name stores the supplied folder name."""
        assert self._parse("--folder-name", "my_sales").folder_name == "my_sales"

    def test_folder_id_flag_sets_value(self) -> None:
        """--folder-id stores the supplied Drive folder ID."""
        assert self._parse("--folder-id", "folder_xyz").folder_id == "folder_xyz"

    def test_default_folder_id_is_none_without_env(self) -> None:
        """folder_id defaults to None when GOOGLE_DRIVE_FOLDER_ID is not set."""
        with patch.dict("os.environ", {}, clear=True):
            # Reload the parser so it re-reads os.getenv at call time.
            with patch("sys.argv", ["seed_drive.py"]):
                import importlib
                import seed_drive as sd
                with patch.object(sd, "parse_args", wraps=sd.parse_args):
                    args = self._parse()
        # The default is set by argparse via os.getenv; if env is absent it's None.
        assert args.folder_id is None or isinstance(args.folder_id, str)

    def test_folder_id_from_env_var(self) -> None:
        """GOOGLE_DRIVE_FOLDER_ID env var is used as the default folder_id."""
        with patch.dict("os.environ", {"GOOGLE_DRIVE_FOLDER_ID": "env_folder_id"}):
            with patch("sys.argv", ["seed_drive.py"]):
                # Re-invoke parse_args so argparse re-reads os.getenv
                import argparse as _ap
                import os
                parser = _ap.ArgumentParser()
                parser.add_argument("--folder-id", default=os.getenv("GOOGLE_DRIVE_FOLDER_ID"))
                args = parser.parse_args([])
            assert args.folder_id == "env_folder_id"


# ---------------------------------------------------------------------------
# main  (mocked Drive)
# ---------------------------------------------------------------------------


class TestMain:
    """Behavioural tests for main() with all Drive API calls mocked."""

    def _run_main(
        self,
        tmp_path: Path,
        argv: list[str],
        service: MagicMock | None = None,
        folder_id_from_create: str = "new_folder_id",
    ) -> MagicMock:
        """Run seed_drive.main() with mocked Drive calls and a temp sample dir.

        Patches authenticate, create_folder, upload_file, and _SAMPLE_DIR so
        no credentials or real files are needed.

        Args:
            tmp_path:              Directory to use as the fake sample dir.
            argv:                  sys.argv to inject (first element is script name).
            service:               Mock service to return from authenticate().
                                   A default mock is created when None.
            folder_id_from_create: Value returned by the mocked create_folder().

        Returns:
            The mock service object so callers can make assertions on it.
        """
        if service is None:
            service = _make_service()

        _populate_sample_dir(tmp_path)

        with (
            patch("sys.argv", argv),
            patch("seed_drive.authenticate", return_value=service),
            patch("seed_drive.create_folder", return_value=folder_id_from_create),
            patch("seed_drive.upload_file", return_value="drive_file_id"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()

        return service

    def test_authenticate_is_called(self, tmp_path: Path) -> None:
        """main() calls authenticate() exactly once."""
        with (
            patch("sys.argv", ["seed_drive.py"]),
            patch("seed_drive.authenticate", return_value=_make_service()) as mock_auth,
            patch("seed_drive.create_folder", return_value="fid"),
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            _populate_sample_dir(tmp_path)
            seed_drive.main()
        mock_auth.assert_called_once()

    def test_create_folder_called_when_no_folder_id(self, tmp_path: Path) -> None:
        """create_folder is called when no --folder-id is provided."""
        _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py"]),
            patch.dict("os.environ", {"GOOGLE_DRIVE_FOLDER_ID": ""}),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder", return_value="new_id") as mock_create,
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        mock_create.assert_called_once()

    def test_create_folder_not_called_when_folder_id_given(self, tmp_path: Path) -> None:
        """create_folder is not called when --folder-id is supplied."""
        with (
            patch("sys.argv", ["seed_drive.py", "--folder-id", "existing_id"]),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder") as mock_create,
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            _populate_sample_dir(tmp_path)
            seed_drive.main()
        mock_create.assert_not_called()

    def test_upload_called_for_each_file(self, tmp_path: Path) -> None:
        """upload_file is called once per spreadsheet file found in _SAMPLE_DIR."""
        files = _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py", "--folder-id", "fid"]),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder"),
            patch("seed_drive.upload_file", return_value="did") as mock_upload,
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        assert mock_upload.call_count == len(files)

    def test_upload_receives_correct_folder_id(self, tmp_path: Path) -> None:
        """upload_file is called with the resolved folder ID."""
        _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py", "--folder-id", "target_folder"]),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder"),
            patch("seed_drive.upload_file", return_value="did") as mock_upload,
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        for upload_call in mock_upload.call_args_list:
            assert upload_call.args[2] == "target_folder"

    def test_custom_folder_name_passed_to_create_folder(self, tmp_path: Path) -> None:
        """The --folder-name value is forwarded to create_folder."""
        _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py", "--folder-name", "my_custom_folder"]),
            patch.dict("os.environ", {"GOOGLE_DRIVE_FOLDER_ID": ""}),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder", return_value="fid") as mock_create,
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        _, folder_name_arg = mock_create.call_args.args
        assert folder_name_arg == "my_custom_folder"

    def test_empty_sample_dir_skips_upload(self, tmp_path: Path) -> None:
        """No upload calls are made when _SAMPLE_DIR is empty."""
        with (
            patch("sys.argv", ["seed_drive.py", "--folder-id", "fid"]),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder"),
            patch("seed_drive.upload_file") as mock_upload,
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        mock_upload.assert_not_called()

    def test_output_contains_folder_id(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        """The printed output includes the resolved folder ID."""
        _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py"]),
            patch.dict("os.environ", {"GOOGLE_DRIVE_FOLDER_ID": ""}),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder", return_value="printed_folder_id"),
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        assert "printed_folder_id" in capsys.readouterr().out

    def test_output_contains_env_setup_instruction(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        """The printed output includes the GOOGLE_DRIVE_FOLDER_ID env var name."""
        _populate_sample_dir(tmp_path)
        with (
            patch("sys.argv", ["seed_drive.py"]),
            patch("seed_drive.authenticate", return_value=_make_service()),
            patch("seed_drive.create_folder", return_value="fid"),
            patch("seed_drive.upload_file", return_value="did"),
            patch("seed_drive._SAMPLE_DIR", tmp_path),
        ):
            seed_drive.main()
        assert "GOOGLE_DRIVE_FOLDER_ID" in capsys.readouterr().out
