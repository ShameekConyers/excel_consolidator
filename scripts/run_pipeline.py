"""End-to-end pipeline script: consolidate → validate → load into SQLite.

Runs the full pipeline against a local folder of Excel/CSV files or a Google
Drive folder and writes the results to a SQLite database.

Two modes:

    seed (default)
        Processes ``data/sample_files/`` and writes ``data/seed.db``.  The
        database is recreated from scratch each run so ``seed.db`` stays a
        deterministic snapshot of the sample data.  Commit the resulting
        file to Git so the dashboard works immediately on clone.

    full
        Processes a custom local folder (``--input``) or a Google Drive
        folder (``--source gdrive --folder-id <ID>``) and appends rows to
        ``data/output/full.db``.  Useful for the live pipeline.

Usage examples::

    # Seed mode — regenerate data/seed.db from sample files (no auth needed)
    .venv/bin/python scripts/run_pipeline.py

    # Seed mode explicit flags
    .venv/bin/python scripts/run_pipeline.py --mode seed --source local

    # Full mode — process a custom local folder
    .venv/bin/python scripts/run_pipeline.py --mode full --input /path/to/folder

    # Full mode — process a Google Drive folder (requires .env credentials)
    .venv/bin/python scripts/run_pipeline.py --mode full --source gdrive --folder-id YOUR_FOLDER_ID

    # Override the output database path
    .venv/bin/python scripts/run_pipeline.py --db data/my_custom.db
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional

# Make src/ importable without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from consolidator import consolidate
from db_loader import LoadResult, build_summary, load, resolve_db_path
from validator import load_rules, summarize, validate

_PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
_DEFAULT_SAMPLE_DIR: Path = _PROJECT_ROOT / "data" / "sample_files"
_DEFAULT_CONFIG: Path = _PROJECT_ROOT / "config" / "validation_rules.yaml"


# ---------------------------------------------------------------------------
# Input resolution
# ---------------------------------------------------------------------------


def resolve_input_folder(
    source: str,
    local_input: Optional[str],
    folder_id: Optional[str],
    tmp_dir: Path,
) -> Path:
    """Return the local folder path that consolidator.py should process.

    For ``local`` source, returns the path as-is.  For ``gdrive`` source,
    downloads every spreadsheet file from the Drive folder into ``tmp_dir``
    and returns that directory.

    Args:
        source:      ``"local"`` or ``"gdrive"``.
        local_input: Path string for local source (may be None, defaults to
                     sample files directory for seed mode).
        folder_id:   Google Drive folder ID for gdrive source.
        tmp_dir:     Temporary directory to use as the download destination
                     for gdrive mode.

    Returns:
        Path to the local directory containing files ready for consolidation.

    Raises:
        ValueError: If ``source`` is ``"gdrive"`` and ``folder_id`` is absent.
        ValueError: If ``source`` is not ``"local"`` or ``"gdrive"``.
        FileNotFoundError: If the resolved local path does not exist.
    """
    if source == "local":
        folder = Path(local_input) if local_input else _DEFAULT_SAMPLE_DIR
        if not folder.exists():
            raise FileNotFoundError(f"Input folder not found: {folder}")
        return folder

    if source == "gdrive":
        if not folder_id:
            raise ValueError(
                "Google Drive source requires --folder-id. "
                "Run scripts/seed_drive.py first to create a Drive folder, "
                "or set GOOGLE_DRIVE_FOLDER_ID in .env."
            )
        return _download_from_drive(folder_id, tmp_dir)

    raise ValueError(f"Unknown source '{source}'. Expected 'local' or 'gdrive'.")


def _download_from_drive(folder_id: str, tmp_dir: Path) -> Path:
    """Download all spreadsheet files from a Drive folder to a local directory.

    Imports Drive connector modules only when needed so the script stays
    fully functional without Google credentials when running in local mode.

    Args:
        folder_id: Google Drive folder ID to download files from.
        tmp_dir:   Local directory where downloaded files are placed.

    Returns:
        ``tmp_dir`` after all files have been written to it.

    Raises:
        SystemExit: If Drive credentials are not found, with a plain-English
                    message pointing to the README setup instructions.
    """
    try:
        from drive_connector import authenticate, download_file, list_files
    except ImportError as exc:
        print(
            "Google Drive dependencies are not installed. "
            "Run: .venv/bin/pip install google-auth google-auth-oauthlib "
            "google-auth-httplib2 google-api-python-client python-dotenv",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    try:
        print("Authenticating with Google Drive...")
        service = authenticate()
    except Exception as exc:
        print(
            f"Drive authentication failed: {exc}\n"
            "Credentials not found. Run in local mode or see README for "
            "Google Drive setup instructions.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    files = list_files(service, folder_id)
    if not files:
        print(
            f"No spreadsheet files found in Drive folder {folder_id}.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"Downloading {len(files)} file(s) from Drive folder {folder_id}...")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for meta in files:
        dest = tmp_dir / meta["name"]
        download_file(service, meta["id"], dest)
        print(f"  Downloaded: {meta['name']}")

    return tmp_dir


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------


def run_pipeline(
    input_folder: Path,
    db_path: Path,
    mode: str,
    config_path: Path,
) -> tuple[str, LoadResult]:
    """Run consolidate → validate → load and return summary strings.

    Args:
        input_folder: Local directory of Excel/CSV files to process.
        db_path:      SQLite file path to write results into.
        mode:         ``"seed"`` (wipe and recreate) or ``"full"`` (append).
        config_path:  Path to ``validation_rules.yaml``.

    Returns:
        A tuple of:
          - validation_summary: Plain-English string from ``validator.summarize()``.
          - load_result:        ``LoadResult`` from ``db_loader.load()``.
    """
    print(f"Reading files from: {input_folder}")
    merged_df, cleaning_log = consolidate(input_folder)
    print(f"  {len(merged_df)} rows consolidated from source files.")

    rules = load_rules(config_path)
    clean_df, quarantine_df = validate(merged_df, rules)
    validation_summary = summarize(clean_df, quarantine_df)

    load_result = load(clean_df, quarantine_df, cleaning_log, db_path, mode=mode)
    return validation_summary, load_result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured ``ArgumentParser`` for the pipeline runner.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Run the full Excel consolidation pipeline "
            "(consolidate → validate → load) and write results to SQLite."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Regenerate seed.db from sample files (default)\n"
            "  .venv/bin/python scripts/run_pipeline.py\n\n"
            "  # Full mode — custom local folder\n"
            "  .venv/bin/python scripts/run_pipeline.py --mode full --input /path/to/folder\n\n"
            "  # Full mode — Google Drive folder\n"
            "  .venv/bin/python scripts/run_pipeline.py --mode full --source gdrive --folder-id YOUR_ID\n"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["seed", "full"],
        default="seed",
        help=(
            "Load mode. 'seed' drops and recreates the database (default). "
            "'full' appends to an existing database."
        ),
    )
    parser.add_argument(
        "--source",
        choices=["local", "gdrive"],
        default="local",
        help="File source. 'local' reads from a folder on disk (default). "
             "'gdrive' downloads from a Google Drive folder (requires --folder-id).",
    )
    parser.add_argument(
        "--input",
        default=None,
        metavar="PATH",
        help=(
            "Path to a local folder of Excel/CSV files. "
            f"Defaults to data/sample_files/ in seed mode."
        ),
    )
    parser.add_argument(
        "--folder-id",
        default=os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
        metavar="ID",
        help=(
            "Google Drive folder ID. Required when --source gdrive. "
            "Defaults to GOOGLE_DRIVE_FOLDER_ID from .env."
        ),
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help=(
            "Override the output SQLite path. "
            "Defaults to data/seed.db (seed mode) or data/output/full.db (full mode)."
        ),
    )
    parser.add_argument(
        "--config",
        default=str(_DEFAULT_CONFIG),
        metavar="PATH",
        help=f"Path to validation_rules.yaml (default: {_DEFAULT_CONFIG}).",
    )
    return parser


def main() -> None:
    """Parse arguments, resolve the input folder, and run the pipeline.

    Prints the validator summary and database summary on completion.
    Exits with code 1 on any unrecoverable error.
    """
    parser = _build_arg_parser()
    args = parser.parse_args()

    db_path = (
        Path(args.db)
        if args.db
        else resolve_db_path(_PROJECT_ROOT, args.mode)
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        try:
            input_folder = resolve_input_folder(
                source=args.source,
                local_input=args.input,
                folder_id=args.folder_id,
                tmp_dir=tmp_dir,
            )
        except (ValueError, FileNotFoundError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

        try:
            validation_summary, load_result = run_pipeline(
                input_folder=input_folder,
                db_path=db_path,
                mode=args.mode,
                config_path=Path(args.config),
            )
        except FileNotFoundError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc

    print()
    print(validation_summary)
    print()
    print(build_summary(load_result))


if __name__ == "__main__":
    main()
