"""
drive_connector.py — Google Drive integration for the Excel consolidation pipeline.

Provides five public functions:
    authenticate()                                      → Drive API service resource
    list_files(service, folder_id)                      → list of file metadata dicts
    download_file(service, file_id, path)               → local Path
    upload_file(service, local_path, folder_id)         → Drive file ID string
    read_file(service, file_id)                         → raw bytes (no local write)
    create_folder(service, folder_name, parent_id=None) → Drive folder ID string

Auth strategy (tried in order):
  1. credentials.json in the project root  (standard GCP OAuth client secret file)
  2. GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET env vars (loaded from .env)

On first auth the browser opens for user consent; token.json is written automatically
and reused (with silent refresh) on every subsequent call.

Neither credentials.json nor token.json are ever committed to Git.
"""

import io
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/drive"]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CREDENTIALS_FILE = _PROJECT_ROOT / "credentials.json"
_TOKEN_FILE = _PROJECT_ROOT / "token.json"

# Only Excel and CSV MIME types are relevant to the pipeline.
_ALLOWED_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel",                                            # .xls
    "text/csv",                                                            # .csv
    "text/plain",                                                          # .csv (alt)
}


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def authenticate() -> Any:
    """Return an authenticated Drive API service resource.

    Raises RuntimeError with a plain-English message if credentials are not
    configured so the caller can handle it gracefully.
    """
    creds = _load_existing_token()

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            _save_token(creds)
        else:
            creds = _run_oauth_flow()

    return build("drive", "v3", credentials=creds)


def _load_existing_token() -> Credentials | None:
    """Load a saved OAuth token from token.json if it exists.

    Returns:
        Credentials loaded from token.json, or None if the file is absent.
    """
    if _TOKEN_FILE.exists():
        return Credentials.from_authorized_user_file(str(_TOKEN_FILE), SCOPES)
    return None


def _save_token(creds: Any) -> None:
    """Persist OAuth credentials to token.json for reuse on future runs.

    Args:
        creds: Google OAuth credentials object with a to_json() method.
    """
    _TOKEN_FILE.write_text(creds.to_json())


def _run_oauth_flow() -> Any:
    """Run the browser-based OAuth consent flow. Tries credentials.json first,
    then falls back to GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET env vars."""
    if _CREDENTIALS_FILE.exists():
        flow = InstalledAppFlow.from_client_secrets_file(
            str(_CREDENTIALS_FILE), SCOPES
        )
    else:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "Google Drive credentials not found.\n"
                "Either place credentials.json in the project root OR set\n"
                "GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in your .env file.\n"
                "See .env.example for setup instructions."
            )
        client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)

    creds = flow.run_local_server(port=0)
    _save_token(creds)
    return creds


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

def list_files(service: Any, folder_id: str) -> list[dict[str, str]]:
    """Return metadata for all .xlsx / .xls / .csv files in folder_id.

    Args:
        service:    Authenticated Drive API resource (from authenticate()).
        folder_id:  Drive folder ID string.

    Returns:
        List of dicts: [{"id": ..., "name": ..., "mimeType": ...}, ...]
    """
    results = []
    page_token = None

    query = f"'{folder_id}' in parents and trashed = false"

    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )

        for f in response.get("files", []):
            if f["mimeType"] in _ALLOWED_MIME_TYPES or _has_spreadsheet_extension(f["name"]):
                results.append(f)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return results


def _has_spreadsheet_extension(name: str) -> bool:
    """Return True if the filename ends with a spreadsheet extension.

    Args:
        name: Filename string to check (e.g. "report.xlsx").

    Returns:
        True for .xlsx, .xls, or .csv (case-insensitive); False otherwise.
    """
    return Path(name).suffix.lower() in {".xlsx", ".xls", ".csv"}


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(service: Any, file_id: str, dest_path: str | Path) -> Path:
    """Download a Drive file to dest_path.

    Args:
        service:    Authenticated Drive API resource.
        file_id:    Drive file ID string.
        dest_path:  Local path (str or Path) to write the file.

    Returns:
        Path object pointing to the downloaded file.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    request = service.files().get_media(fileId=file_id)

    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return dest_path


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_file(service: Any, local_path: str | Path, folder_id: str) -> str:
    """Upload a local file to a Drive folder.

    Args:
        service:     Authenticated Drive API resource.
        local_path:  Local file path (str or Path).
        folder_id:   Destination Drive folder ID.

    Returns:
        Drive file ID string of the newly uploaded file.
    """
    local_path = Path(local_path)
    file_metadata = {"name": local_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(local_path), resumable=True)

    uploaded = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    return uploaded["id"]


# ---------------------------------------------------------------------------
# Create folder
# ---------------------------------------------------------------------------

def create_folder(service: Any, folder_name: str, parent_id: str | None = None) -> str:
    """Create a new folder in Google Drive and return its file ID.

    Args:
        service:      Authenticated Drive API resource (from authenticate()).
        folder_name:  Display name for the new folder.
        parent_id:    Optional Drive folder ID to nest the new folder inside.
                      If None, the folder is created in the user's root Drive.

    Returns:
        Drive file ID string of the newly created folder.
    """
    metadata: dict[str, object] = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    folder = (
        service.files()
        .create(body=metadata, fields="id")
        .execute()
    )
    return folder["id"]


# ---------------------------------------------------------------------------
# Read (in-memory, no local write)
# ---------------------------------------------------------------------------

def read_file(service: Any, file_id: str) -> bytes:
    """Return raw bytes for a Drive file without writing to disk.

    Args:
        service:  Authenticated Drive API resource.
        file_id:  Drive file ID string.

    Returns:
        bytes — full file content.
    """
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    print("drive_connector.py — Google Drive integration module")
    print("Available functions:")
    print("  authenticate()                                      → Drive service resource")
    print("  list_files(service, folder_id)                      → list of file dicts")
    print("  download_file(service, file_id, path)               → local Path")
    print("  upload_file(service, local_path, folder_id)         → Drive file ID")
    print("  read_file(service, file_id)                         → bytes")
    print("  create_folder(service, folder_name, parent_id=None) → Drive folder ID")
    print()
    print("Quick auth test (requires credentials.json or .env):")
    print("  python src/drive_connector.py --auth")

    if len(sys.argv) > 1 and sys.argv[1] == "--auth":
        try:
            svc = authenticate()
            about = svc.about().get(fields="user").execute()
            print(f"\nAuthenticated as: {about['user']['emailAddress']}")
        except RuntimeError as e:
            print(f"\nAuth failed: {e}")
            sys.exit(1)
