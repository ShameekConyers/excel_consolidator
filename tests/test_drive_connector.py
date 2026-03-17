"""
Unit tests for src/drive_connector.py.

All Google Drive API calls are mocked — no credentials or network access required.
Run with: pytest tests/test_drive_connector.py -v
"""

import os
import sys
from pathlib import Path
from typing import Any, get_type_hints
from unittest.mock import MagicMock, call, patch

import pytest

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import drive_connector
from drive_connector import (
    _has_spreadsheet_extension,
    authenticate,
    create_folder,
    download_file,
    list_files,
    read_file,
    upload_file,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service():
    """Return a MagicMock wired up to mirror the Drive API call chain."""
    return MagicMock()


# ---------------------------------------------------------------------------
# _has_spreadsheet_extension
# ---------------------------------------------------------------------------

class TestHasSpreadsheetExtension:
    """Tests for the _has_spreadsheet_extension filename filter."""

    def test_xlsx_returns_true(self):
        assert _has_spreadsheet_extension("report.xlsx") is True

    def test_xls_returns_true(self):
        assert _has_spreadsheet_extension("report.xls") is True

    def test_csv_returns_true(self):
        assert _has_spreadsheet_extension("data.csv") is True

    def test_uppercase_extension_returns_true(self):
        assert _has_spreadsheet_extension("REPORT.XLSX") is True

    def test_mixed_case_extension_returns_true(self):
        assert _has_spreadsheet_extension("Data.Csv") is True

    def test_txt_returns_false(self):
        assert _has_spreadsheet_extension("notes.txt") is False

    def test_pdf_returns_false(self):
        assert _has_spreadsheet_extension("report.pdf") is False

    def test_docx_returns_false(self):
        assert _has_spreadsheet_extension("doc.docx") is False

    def test_no_extension_returns_false(self):
        assert _has_spreadsheet_extension("noextension") is False

    def test_path_with_dots_in_name_uses_final_extension(self):
        # e.g. "report.v2.xlsx" — only the final suffix counts
        assert _has_spreadsheet_extension("report.v2.xlsx") is True
        assert _has_spreadsheet_extension("report.v2.txt") is False


# ---------------------------------------------------------------------------
# list_files
# ---------------------------------------------------------------------------

class TestListFiles:
    """Tests for list_files() — Drive folder listing and spreadsheet filtering."""

    def _response(self, files, next_token=None):
        r = {"files": files}
        if next_token:
            r["nextPageToken"] = next_token
        return r

    def test_returns_xlsx_files(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([
            {"id": "1", "name": "Q1.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
        ])
        result = list_files(service, "folder_abc")
        assert len(result) == 1
        assert result[0]["name"] == "Q1.xlsx"

    def test_returns_csv_files(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([
            {"id": "2", "name": "data.csv", "mimeType": "text/csv"},
        ])
        result = list_files(service, "folder_abc")
        assert len(result) == 1
        assert result[0]["name"] == "data.csv"

    def test_returns_file_with_spreadsheet_extension_regardless_of_mimetype(self):
        # Some Drive uploads have a generic mimeType but the filename ends in .xlsx
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([
            {"id": "3", "name": "upload.xlsx", "mimeType": "application/octet-stream"},
        ])
        result = list_files(service, "folder_abc")
        assert len(result) == 1

    def test_filters_out_non_spreadsheet_files(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([
            {"id": "4", "name": "image.png", "mimeType": "image/png"},
            {"id": "5", "name": "doc.pdf", "mimeType": "application/pdf"},
            {"id": "6", "name": "notes.docx", "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
        ])
        result = list_files(service, "folder_abc")
        assert result == []

    def test_returns_empty_list_for_empty_folder(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([])
        result = list_files(service, "folder_abc")
        assert result == []

    def test_handles_pagination(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.side_effect = [
            self._response(
                [{"id": "1", "name": "page1.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}],
                next_token="token_page2",
            ),
            self._response(
                [{"id": "2", "name": "page2.csv", "mimeType": "text/csv"}],
            ),
        ]
        result = list_files(service, "folder_abc")
        assert len(result) == 2
        assert result[0]["name"] == "page1.xlsx"
        assert result[1]["name"] == "page2.csv"

    def test_mixed_valid_and_invalid_files_returns_only_valid(self):
        service = _make_service()
        service.files.return_value.list.return_value.execute.return_value = self._response([
            {"id": "1", "name": "sales.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            {"id": "2", "name": "photo.jpg", "mimeType": "image/jpeg"},
            {"id": "3", "name": "data.csv", "mimeType": "text/csv"},
        ])
        result = list_files(service, "folder_abc")
        names = [f["name"] for f in result]
        assert "sales.xlsx" in names
        assert "data.csv" in names
        assert "photo.jpg" not in names

    def test_returns_all_metadata_fields(self):
        service = _make_service()
        file_entry = {"id": "abc", "name": "report.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        service.files.return_value.list.return_value.execute.return_value = self._response([file_entry])
        result = list_files(service, "folder_abc")
        assert result[0] == file_entry


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------

class TestDownloadFile:
    """Tests for download_file() — streaming a Drive file to local disk."""

    @patch("drive_connector.MediaIoBaseDownload")
    def test_returns_path_object(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_instance = MagicMock()
        mock_dl_class.return_value = mock_dl_instance
        mock_dl_instance.next_chunk.return_value = (None, True)

        result = download_file(service, "file123", tmp_path / "out.xlsx")
        assert isinstance(result, Path)

    @patch("drive_connector.MediaIoBaseDownload")
    def test_returned_path_matches_dest_path(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        dest = tmp_path / "result.xlsx"
        result = download_file(service, "file123", dest)
        assert result == dest

    @patch("drive_connector.MediaIoBaseDownload")
    def test_file_is_created_on_disk(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        dest = tmp_path / "result.xlsx"
        download_file(service, "file123", dest)
        assert dest.exists()

    @patch("drive_connector.MediaIoBaseDownload")
    def test_accepts_string_dest_path(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        dest = str(tmp_path / "result.xlsx")
        result = download_file(service, "file123", dest)
        assert result == Path(dest)

    @patch("drive_connector.MediaIoBaseDownload")
    def test_creates_parent_directories(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        dest = tmp_path / "a" / "b" / "c" / "result.xlsx"
        download_file(service, "file123", dest)
        assert dest.exists()

    @patch("drive_connector.MediaIoBaseDownload")
    def test_calls_get_media_with_correct_file_id(self, mock_dl_class, tmp_path):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        download_file(service, "my_file_id", tmp_path / "out.xlsx")
        service.files.return_value.get_media.assert_called_once_with(fileId="my_file_id")

    @patch("drive_connector.MediaIoBaseDownload")
    def test_loops_until_done(self, mock_dl_class, tmp_path):
        """next_chunk() returning done=False on first call should loop until True."""
        service = _make_service()
        mock_dl_instance = MagicMock()
        mock_dl_class.return_value = mock_dl_instance
        mock_dl_instance.next_chunk.side_effect = [
            (MagicMock(), False),
            (MagicMock(), False),
            (MagicMock(), True),
        ]

        download_file(service, "file123", tmp_path / "out.xlsx")
        assert mock_dl_instance.next_chunk.call_count == 3


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------

class TestUploadFile:
    """Tests for upload_file() — sending a local file to a Drive folder."""

    @patch("drive_connector.MediaFileUpload")
    def test_returns_drive_file_id(self, mock_media_upload, tmp_path):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "new_id_123"}

        local_file = tmp_path / "report.xlsx"
        local_file.write_bytes(b"fake content")

        result = upload_file(service, local_file, "folder_xyz")
        assert result == "new_id_123"

    @patch("drive_connector.MediaFileUpload")
    def test_uses_filename_as_drive_name(self, mock_media_upload, tmp_path):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "id_abc"}

        local_file = tmp_path / "Q1_sales.xlsx"
        local_file.write_bytes(b"content")

        upload_file(service, local_file, "folder_xyz")

        call_kwargs = service.files.return_value.create.call_args
        assert call_kwargs.kwargs["body"]["name"] == "Q1_sales.xlsx"

    @patch("drive_connector.MediaFileUpload")
    def test_sets_correct_parent_folder(self, mock_media_upload, tmp_path):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "id_abc"}

        local_file = tmp_path / "data.csv"
        local_file.write_bytes(b"a,b,c")

        upload_file(service, local_file, "target_folder_id")

        call_kwargs = service.files.return_value.create.call_args
        assert "target_folder_id" in call_kwargs.kwargs["body"]["parents"]

    @patch("drive_connector.MediaFileUpload")
    def test_creates_resumable_media_upload(self, mock_media_upload, tmp_path):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "id_abc"}

        local_file = tmp_path / "data.xlsx"
        local_file.write_bytes(b"content")

        upload_file(service, local_file, "folder")
        mock_media_upload.assert_called_once_with(str(local_file), resumable=True)

    @patch("drive_connector.MediaFileUpload")
    def test_accepts_string_local_path(self, mock_media_upload, tmp_path):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "id_str"}

        local_file = tmp_path / "file.xlsx"
        local_file.write_bytes(b"content")

        result = upload_file(service, str(local_file), "folder")
        assert result == "id_str"


# ---------------------------------------------------------------------------
# read_file
# ---------------------------------------------------------------------------

class TestReadFile:
    """Tests for read_file() — returning Drive file content as bytes without writing to disk."""

    @patch("drive_connector.MediaIoBaseDownload")
    def test_returns_bytes(self, mock_dl_class):
        service = _make_service()

        def fake_downloader(buffer, request):
            buffer.write(b"hello from drive")
            instance = MagicMock()
            instance.next_chunk.return_value = (None, True)
            return instance

        mock_dl_class.side_effect = fake_downloader
        result = read_file(service, "file_abc")
        assert result == b"hello from drive"

    @patch("drive_connector.MediaIoBaseDownload")
    def test_returns_empty_bytes_for_empty_file(self, mock_dl_class):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        result = read_file(service, "empty_file")
        assert result == b""

    @patch("drive_connector.MediaIoBaseDownload")
    def test_calls_get_media_with_correct_file_id(self, mock_dl_class):
        service = _make_service()
        mock_dl_class.return_value.next_chunk.return_value = (None, True)

        read_file(service, "target_file_id")
        service.files.return_value.get_media.assert_called_once_with(fileId="target_file_id")

    @patch("drive_connector.MediaIoBaseDownload")
    def test_loops_until_done(self, mock_dl_class):
        service = _make_service()
        mock_dl_instance = MagicMock()
        mock_dl_class.return_value = mock_dl_instance
        mock_dl_instance.next_chunk.side_effect = [
            (MagicMock(), False),
            (MagicMock(), True),
        ]

        read_file(service, "file_id")
        assert mock_dl_instance.next_chunk.call_count == 2

    @patch("drive_connector.MediaIoBaseDownload")
    def test_returns_binary_content_intact(self, mock_dl_class):
        service = _make_service()
        binary_data = bytes(range(256))

        def fake_downloader(buffer, request):
            buffer.write(binary_data)
            instance = MagicMock()
            instance.next_chunk.return_value = (None, True)
            return instance

        mock_dl_class.side_effect = fake_downloader
        result = read_file(service, "bin_file")
        assert result == binary_data


# ---------------------------------------------------------------------------
# _load_existing_token
# ---------------------------------------------------------------------------

class TestLoadExistingToken:
    """Tests for _load_existing_token() — reading a saved OAuth token from disk."""

    @patch("drive_connector._TOKEN_FILE")
    def test_returns_none_when_token_file_missing(self, mock_token_file):
        mock_token_file.exists.return_value = False
        result = drive_connector._load_existing_token()
        assert result is None

    @patch("drive_connector.Credentials")
    @patch("drive_connector._TOKEN_FILE")
    def test_returns_credentials_when_token_file_exists(self, mock_token_file, mock_creds_class):
        mock_token_file.exists.return_value = True
        mock_token_file.__str__ = lambda self: "/fake/token.json"
        mock_creds = MagicMock()
        mock_creds_class.from_authorized_user_file.return_value = mock_creds

        result = drive_connector._load_existing_token()
        assert result is mock_creds

    @patch("drive_connector.Credentials")
    @patch("drive_connector._TOKEN_FILE")
    def test_passes_scopes_when_loading_token(self, mock_token_file, mock_creds_class):
        mock_token_file.exists.return_value = True
        mock_token_file.__str__ = lambda self: "/fake/token.json"

        drive_connector._load_existing_token()

        _, call_args = mock_creds_class.from_authorized_user_file.call_args
        assert drive_connector.SCOPES in call_args.values() or drive_connector.SCOPES in mock_creds_class.from_authorized_user_file.call_args[0]


# ---------------------------------------------------------------------------
# _run_oauth_flow
# ---------------------------------------------------------------------------

class TestRunOauthFlow:
    """Tests for _run_oauth_flow() — credential source selection, error handling, and token persistence."""

    @patch("drive_connector._CREDENTIALS_FILE")
    def test_raises_when_no_credentials_file_and_no_env_vars(self, mock_cred_file):
        mock_cred_file.exists.return_value = False
        with patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "", "GOOGLE_CLIENT_SECRET": ""}):
            with pytest.raises(RuntimeError, match="credentials not found"):
                drive_connector._run_oauth_flow()

    @patch("drive_connector._CREDENTIALS_FILE")
    def test_raises_when_only_client_id_set(self, mock_cred_file):
        mock_cred_file.exists.return_value = False
        with patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "some_id", "GOOGLE_CLIENT_SECRET": ""}):
            with pytest.raises(RuntimeError):
                drive_connector._run_oauth_flow()

    @patch("drive_connector._save_token")
    @patch("drive_connector.InstalledAppFlow")
    @patch("drive_connector._CREDENTIALS_FILE")
    def test_uses_credentials_file_when_present(self, mock_cred_file, mock_flow_class, mock_save_token):
        mock_cred_file.exists.return_value = True
        mock_cred_file.__str__ = lambda self: "/fake/credentials.json"
        mock_flow_instance = MagicMock()
        mock_flow_class.from_client_secrets_file.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = MagicMock()

        drive_connector._run_oauth_flow()

        mock_flow_class.from_client_secrets_file.assert_called_once()
        mock_flow_class.from_client_config.assert_not_called()

    @patch("drive_connector._save_token")
    @patch("drive_connector.InstalledAppFlow")
    @patch("drive_connector._CREDENTIALS_FILE")
    def test_uses_env_vars_when_no_credentials_file(self, mock_cred_file, mock_flow_class, mock_save_token):
        mock_cred_file.exists.return_value = False
        mock_flow_instance = MagicMock()
        mock_flow_class.from_client_config.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = MagicMock()

        with patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "cid", "GOOGLE_CLIENT_SECRET": "csecret"}):
            drive_connector._run_oauth_flow()

        mock_flow_class.from_client_config.assert_called_once()
        mock_flow_class.from_client_secrets_file.assert_not_called()

    @patch("drive_connector._save_token")
    @patch("drive_connector.InstalledAppFlow")
    @patch("drive_connector._CREDENTIALS_FILE")
    def test_env_var_config_includes_client_id_and_secret(self, mock_cred_file, mock_flow_class, mock_save_token):
        mock_cred_file.exists.return_value = False
        mock_flow_instance = MagicMock()
        mock_flow_class.from_client_config.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = MagicMock()

        with patch.dict(os.environ, {"GOOGLE_CLIENT_ID": "my_client_id", "GOOGLE_CLIENT_SECRET": "my_client_secret"}):
            drive_connector._run_oauth_flow()

        config_arg = mock_flow_class.from_client_config.call_args[0][0]
        assert config_arg["installed"]["client_id"] == "my_client_id"
        assert config_arg["installed"]["client_secret"] == "my_client_secret"

    @patch("drive_connector._save_token")
    @patch("drive_connector.InstalledAppFlow")
    @patch("drive_connector._CREDENTIALS_FILE")
    def test_runs_local_server_on_port_0(self, mock_cred_file, mock_flow_class, mock_save_token):
        mock_cred_file.exists.return_value = True
        mock_flow_instance = MagicMock()
        mock_flow_class.from_client_secrets_file.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = MagicMock()

        drive_connector._run_oauth_flow()
        mock_flow_instance.run_local_server.assert_called_once_with(port=0)

    @patch("drive_connector._save_token")
    @patch("drive_connector.InstalledAppFlow")
    @patch("drive_connector._CREDENTIALS_FILE")
    def test_saves_token_after_flow(self, mock_cred_file, mock_flow_class, mock_save_token):
        mock_cred_file.exists.return_value = True
        mock_flow_instance = MagicMock()
        mock_creds = MagicMock()
        mock_flow_class.from_client_secrets_file.return_value = mock_flow_instance
        mock_flow_instance.run_local_server.return_value = mock_creds

        drive_connector._run_oauth_flow()
        mock_save_token.assert_called_once_with(mock_creds)


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------

class TestAuthenticate:
    """Tests for authenticate() — token reuse, refresh, and OAuth flow fallback."""

    @patch("drive_connector.build")
    @patch("drive_connector._load_existing_token")
    def test_uses_valid_token_without_refresh(self, mock_load_token, mock_build):
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_load_token.return_value = mock_creds

        result = authenticate()

        mock_build.assert_called_once_with("drive", "v3", credentials=mock_creds)
        assert result == mock_build.return_value

    @patch("drive_connector.build")
    @patch("drive_connector._save_token")
    @patch("drive_connector._load_existing_token")
    def test_refreshes_expired_token(self, mock_load_token, mock_save_token, mock_build):
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "some_refresh_token"
        mock_load_token.return_value = mock_creds

        authenticate()

        mock_creds.refresh.assert_called_once()
        mock_save_token.assert_called_once_with(mock_creds)

    @patch("drive_connector.build")
    @patch("drive_connector._save_token")
    @patch("drive_connector._load_existing_token")
    def test_builds_service_after_token_refresh(self, mock_load_token, mock_save_token, mock_build):
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "refresh"
        mock_load_token.return_value = mock_creds

        result = authenticate()

        mock_build.assert_called_once_with("drive", "v3", credentials=mock_creds)
        assert result == mock_build.return_value

    @patch("drive_connector.build")
    @patch("drive_connector._run_oauth_flow")
    @patch("drive_connector._load_existing_token")
    def test_runs_oauth_flow_when_no_token(self, mock_load_token, mock_run_flow, mock_build):
        mock_load_token.return_value = None
        mock_new_creds = MagicMock()
        mock_run_flow.return_value = mock_new_creds

        authenticate()

        mock_run_flow.assert_called_once()
        mock_build.assert_called_once_with("drive", "v3", credentials=mock_new_creds)

    @patch("drive_connector.build")
    @patch("drive_connector._run_oauth_flow")
    @patch("drive_connector._load_existing_token")
    def test_runs_oauth_flow_when_token_invalid_no_refresh(self, mock_load_token, mock_run_flow, mock_build):
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = False   # not expired, just invalid — no refresh possible
        mock_load_token.return_value = mock_creds
        mock_run_flow.return_value = MagicMock()

        authenticate()

        mock_run_flow.assert_called_once()

    @patch("drive_connector.build")
    @patch("drive_connector._run_oauth_flow")
    @patch("drive_connector._load_existing_token")
    def test_returns_drive_service_resource(self, mock_load_token, mock_run_flow, mock_build):
        mock_load_token.return_value = None
        mock_run_flow.return_value = MagicMock()
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        result = authenticate()
        assert result is mock_service


# ---------------------------------------------------------------------------
# create_folder
# ---------------------------------------------------------------------------

class TestCreateFolder:
    """Tests for create_folder() — creating a new Drive folder."""

    def test_returns_folder_id_string(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "folder_id_abc"}

        result = create_folder(service, "My Folder")
        assert result == "folder_id_abc"

    def test_sets_folder_mimetype(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "fid"}

        create_folder(service, "Test Folder")

        call_kwargs = service.files.return_value.create.call_args
        assert call_kwargs.kwargs["body"]["mimeType"] == "application/vnd.google-apps.folder"

    def test_sets_folder_name(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "fid"}

        create_folder(service, "Sales 2024")

        call_kwargs = service.files.return_value.create.call_args
        assert call_kwargs.kwargs["body"]["name"] == "Sales 2024"

    def test_sets_parent_when_provided(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "fid"}

        create_folder(service, "Sub Folder", parent_id="parent_folder_id")

        call_kwargs = service.files.return_value.create.call_args
        assert "parent_folder_id" in call_kwargs.kwargs["body"]["parents"]

    def test_no_parents_key_when_parent_id_is_none(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "fid"}

        create_folder(service, "Root Folder")

        call_kwargs = service.files.return_value.create.call_args
        assert "parents" not in call_kwargs.kwargs["body"]

    def test_requests_id_field_only(self):
        service = _make_service()
        service.files.return_value.create.return_value.execute.return_value = {"id": "fid"}

        create_folder(service, "My Folder")

        call_kwargs = service.files.return_value.create.call_args
        assert call_kwargs.kwargs["fields"] == "id"


# ---------------------------------------------------------------------------
# Type annotations
# ---------------------------------------------------------------------------

class TestTypeAnnotations:
    """Verify that every public function carries the expected type annotations.

    These tests catch regressions where a signature is accidentally changed or
    a type hint is removed during refactoring.
    """

    def _hints(self, fn):
        return get_type_hints(fn)

    # list_files
    def test_list_files_folder_id_is_str(self):
        assert self._hints(list_files)["folder_id"] is str

    def test_list_files_returns_list_of_dicts(self):
        assert self._hints(list_files)["return"] == list[dict[str, str]]

    # download_file
    def test_download_file_file_id_is_str(self):
        assert self._hints(download_file)["file_id"] is str

    def test_download_file_dest_path_accepts_str_or_path(self):
        assert self._hints(download_file)["dest_path"] == str | Path

    def test_download_file_returns_path(self):
        assert self._hints(download_file)["return"] is Path

    # upload_file
    def test_upload_file_local_path_accepts_str_or_path(self):
        assert self._hints(upload_file)["local_path"] == str | Path

    def test_upload_file_folder_id_is_str(self):
        assert self._hints(upload_file)["folder_id"] is str

    def test_upload_file_returns_str(self):
        assert self._hints(upload_file)["return"] is str

    # read_file
    def test_read_file_file_id_is_str(self):
        assert self._hints(read_file)["file_id"] is str

    def test_read_file_returns_bytes(self):
        assert self._hints(read_file)["return"] is bytes

    # service parameters typed as Any
    def test_list_files_service_is_any(self):
        assert self._hints(list_files)["service"] is Any

    def test_download_file_service_is_any(self):
        assert self._hints(download_file)["service"] is Any

    def test_upload_file_service_is_any(self):
        assert self._hints(upload_file)["service"] is Any

    def test_read_file_service_is_any(self):
        assert self._hints(read_file)["service"] is Any

    # create_folder
    def test_create_folder_folder_name_is_str(self):
        assert self._hints(create_folder)["folder_name"] is str

    def test_create_folder_returns_str(self):
        assert self._hints(create_folder)["return"] is str

    def test_create_folder_service_is_any(self):
        assert self._hints(create_folder)["service"] is Any
