"""
scripts/seed_drive.py — Upload local sample files to a new Google Drive folder.

Usage:
    python scripts/seed_drive.py
    python scripts/seed_drive.py --folder-name "my_sales_data"
    python scripts/seed_drive.py --folder-id <DRIVE_FOLDER_ID>

What it does:
    1. Authenticates with Google Drive (uses credentials.json or .env vars).
    2. If --folder-id is given (or GOOGLE_DRIVE_FOLDER_ID is in .env), uploads directly
       to that existing folder — no new folder is created.
    3. Otherwise, creates a new folder (default name: "excel_consolidator_samples")
       in the Drive root and uploads there.
    4. Prints the folder ID — paste this as GOOGLE_DRIVE_FOLDER_ID in your .env.

Run this once to set up the Drive source for end-to-end testing with --source gdrive.
"""

import argparse
import os
import sys
from pathlib import Path

# Make src/ importable without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from drive_connector import authenticate, create_folder, upload_file

_SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample_files"
_DEFAULT_FOLDER_NAME = "excel_consolidator_samples"
_SPREADSHEET_EXTENSIONS = {".xlsx", ".xls", ".csv"}


def collect_sample_files(sample_dir: Path) -> list[Path]:
    """Return all spreadsheet files in sample_dir (non-recursive).

    Args:
        sample_dir: Local directory containing sample Excel/CSV files.

    Returns:
        Sorted list of Path objects for .xlsx, .xls, and .csv files.
    """
    return sorted(
        f for f in sample_dir.iterdir()
        if f.is_file() and f.suffix.lower() in _SPREADSHEET_EXTENSIONS
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the Drive seeder.

    --folder-id defaults to GOOGLE_DRIVE_FOLDER_ID from .env if present.
    When a folder ID is resolved, files are uploaded directly to that folder
    and no new folder is created.

    Returns:
        Namespace with folder_name (str) and folder_id (str | None).
    """
    parser = argparse.ArgumentParser(
        description="Upload local sample files to a Google Drive folder."
    )
    parser.add_argument(
        "--folder-name",
        default=_DEFAULT_FOLDER_NAME,
        help=(
            f"Name for a new Drive folder to create (default: '{_DEFAULT_FOLDER_NAME}'). "
            "Ignored when --folder-id is provided or GOOGLE_DRIVE_FOLDER_ID is set in .env."
        ),
    )
    parser.add_argument(
        "--folder-id",
        default=os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
        help=(
            "Existing Drive folder ID to upload files into directly. "
            "Defaults to GOOGLE_DRIVE_FOLDER_ID from .env. "
            "If unset, a new folder is created using --folder-name."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Authenticate, resolve the target Drive folder, upload all sample files, and print the folder ID."""
    args = parse_args()

    print("Authenticating with Google Drive...")
    service = authenticate()
    about = service.about().get(fields="user").execute()
    print(f"Authenticated as: {about['user']['emailAddress']}\n")

    if args.folder_id:
        folder_id = args.folder_id
        print(f"Using existing Drive folder: {folder_id}\n")
    else:
        print(f"Creating Drive folder: '{args.folder_name}'...")
        folder_id = create_folder(service, args.folder_name)
        print(f"Folder created. ID: {folder_id}\n")

    files = collect_sample_files(_SAMPLE_DIR)
    if not files:
        print(f"No spreadsheet files found in {_SAMPLE_DIR}. Nothing to upload.")
        return

    print(f"Uploading {len(files)} file(s) to Drive...")
    for local_path in files:
        drive_id = upload_file(service, local_path, folder_id)
        print(f"  Uploaded: {local_path.name}  →  Drive ID: {drive_id}")

    print(f"""
Done. Add this to your .env:

    GOOGLE_DRIVE_FOLDER_ID={folder_id}

Then run the pipeline with:

    .venv/bin/python src/consolidator.py --source gdrive --folder-id {folder_id}
""")


if __name__ == "__main__":
    main()
