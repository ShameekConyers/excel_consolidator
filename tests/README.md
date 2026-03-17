# Tests

Run all tests: `pytest tests/ -v`

---

## `test_drive_connector.py`

Unit tests for `src/drive_connector.py`. All Google Drive API calls are mocked —
no credentials or network access required.

---

### `TestHasSpreadsheetExtension`

Tests the private helper that decides whether a filename is a spreadsheet.

| Test | Covers |
|------|--------|
| `test_xlsx_returns_true` | `.xlsx` extension is accepted |
| `test_xls_returns_true` | `.xls` extension is accepted |
| `test_csv_returns_true` | `.csv` extension is accepted |
| `test_uppercase_extension_returns_true` | Extension check is case-insensitive (`.XLSX`) |
| `test_mixed_case_extension_returns_true` | Extension check is case-insensitive (`.Csv`) |
| `test_txt_returns_false` | `.txt` is rejected |
| `test_pdf_returns_false` | `.pdf` is rejected |
| `test_docx_returns_false` | `.docx` is rejected |
| `test_no_extension_returns_false` | Filenames with no extension are rejected |
| `test_path_with_dots_in_name_uses_final_extension` | Only the final suffix counts (`report.v2.xlsx` → accepted, `report.v2.txt` → rejected) |

---

### `TestListFiles`

Tests that `list_files()` correctly pages through Drive results and filters to
spreadsheet files only.

| Test | Covers |
|------|--------|
| `test_returns_xlsx_files` | Files with the `.xlsx` MIME type are included |
| `test_returns_csv_files` | Files with the `text/csv` MIME type are included |
| `test_returns_file_with_spreadsheet_extension_regardless_of_mimetype` | A file with a `.xlsx` extension but a generic MIME type (`application/octet-stream`) is still included |
| `test_filters_out_non_spreadsheet_files` | `.png`, `.pdf`, and `.docx` files are excluded |
| `test_returns_empty_list_for_empty_folder` | Empty folder returns an empty list without error |
| `test_handles_pagination` | When the API returns a `nextPageToken`, a second request is made and both pages are combined |
| `test_mixed_valid_and_invalid_files_returns_only_valid` | A response containing both spreadsheets and non-spreadsheets returns only the spreadsheets |
| `test_returns_all_metadata_fields` | The full metadata dict (`id`, `name`, `mimeType`) is returned unchanged |

---

### `TestDownloadFile`

Tests that `download_file()` streams a Drive file to disk correctly.

| Test | Covers |
|------|--------|
| `test_returns_path_object` | Return type is `pathlib.Path` |
| `test_returned_path_matches_dest_path` | Returned path equals the `dest_path` argument |
| `test_file_is_created_on_disk` | The destination file exists on disk after the call |
| `test_accepts_string_dest_path` | `dest_path` can be a plain string, not just a `Path` |
| `test_creates_parent_directories` | Missing parent directories are created automatically |
| `test_calls_get_media_with_correct_file_id` | `get_media()` is called with the exact `file_id` passed in |
| `test_loops_until_done` | When `next_chunk()` returns `done=False` multiple times, the loop continues until `done=True` |

---

### `TestUploadFile`

Tests that `upload_file()` sends the correct metadata to Drive and returns the
new file's ID.

| Test | Covers |
|------|--------|
| `test_returns_drive_file_id` | The Drive file ID string from the API response is returned |
| `test_uses_filename_as_drive_name` | The Drive file name is taken from the local filename, not the full path |
| `test_sets_correct_parent_folder` | The `folder_id` argument is set as the file's parent in Drive |
| `test_creates_resumable_media_upload` | `MediaFileUpload` is called with `resumable=True` |
| `test_accepts_string_local_path` | `local_path` can be a plain string, not just a `Path` |

---

### `TestReadFile`

Tests that `read_file()` returns file content as bytes without writing to disk.

| Test | Covers |
|------|--------|
| `test_returns_bytes` | Return type is `bytes` containing the file content |
| `test_returns_empty_bytes_for_empty_file` | An empty Drive file returns `b""` without error |
| `test_calls_get_media_with_correct_file_id` | `get_media()` is called with the exact `file_id` passed in |
| `test_loops_until_done` | Multi-chunk downloads loop until `done=True` |
| `test_returns_binary_content_intact` | Binary content (all 256 byte values) is returned without corruption |

---

### `TestLoadExistingToken`

Tests the private helper that reads a saved OAuth token from `token.json`.

| Test | Covers |
|------|--------|
| `test_returns_none_when_token_file_missing` | Returns `None` when `token.json` does not exist |
| `test_returns_credentials_when_token_file_exists` | Returns a `Credentials` object when `token.json` exists |
| `test_passes_scopes_when_loading_token` | The correct OAuth scopes are passed when loading credentials |

---

### `TestRunOauthFlow`

Tests the private OAuth consent flow — credential source selection, error handling,
and token persistence.

| Test | Covers |
|------|--------|
| `test_raises_when_no_credentials_file_and_no_env_vars` | `RuntimeError` is raised with a plain-English message when neither `credentials.json` nor env vars are present |
| `test_raises_when_only_client_id_set` | `RuntimeError` is raised when `GOOGLE_CLIENT_ID` is set but `GOOGLE_CLIENT_SECRET` is missing |
| `test_uses_credentials_file_when_present` | `InstalledAppFlow.from_client_secrets_file()` is used when `credentials.json` exists |
| `test_uses_env_vars_when_no_credentials_file` | `InstalledAppFlow.from_client_config()` is used when env vars are set and no `credentials.json` |
| `test_env_var_config_includes_client_id_and_secret` | The client config passed to `from_client_config()` contains the values from `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` |
| `test_runs_local_server_on_port_0` | `run_local_server(port=0)` is called (OS picks a free port) |
| `test_saves_token_after_flow` | `_save_token()` is called with the new credentials after the consent flow completes |

---

### `TestAuthenticate`

Tests the public `authenticate()` function — token reuse, refresh, and fallback
to the OAuth flow.

| Test | Covers |
|------|--------|
| `test_uses_valid_token_without_refresh` | A valid existing token is used directly; no refresh or OAuth flow is triggered |
| `test_refreshes_expired_token` | An expired token with a refresh token is silently refreshed via `creds.refresh()` |
| `test_builds_service_after_token_refresh` | The Drive service is built with the refreshed credentials |
| `test_runs_oauth_flow_when_no_token` | The OAuth flow runs when no `token.json` exists |
| `test_runs_oauth_flow_when_token_invalid_no_refresh` | The OAuth flow runs when a token exists but is invalid and has no refresh token |
| `test_returns_drive_service_resource` | The return value is the Drive service resource from `build()` |
