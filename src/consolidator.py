"""Core module for reading, standardizing, and merging Excel and CSV files.

Handles structural cleaning only. Business-rule validation (min/max bounds,
required fields, quarantine) is handled downstream by validator.py.

Pipeline steps applied to each file:

    1. Discover all .xlsx, .xls, and .csv files in a folder.
    2. Read each file, tolerating title rows above the real header row
       (e.g. west_region_2024.xlsx has two non-data rows before the header).
    3. Standardize column names to one of seven canonical names: date, product,
       region, sales_rep, customer, quantity, revenue. Inject "West" for the
       region column when it is absent and the filename implies a single-region
       file.
    4. Normalize dates to uniform YYYY-MM-DD strings from ISO, US
       (MM/DD/YYYY), and written-month formats (e.g. "March 15, 2024").
    5. Clean numeric columns by stripping currency symbols, commas, and
       whitespace. Non-parseable values (e.g. "pending", "TBD") are left
       unchanged so the validator can quarantine them with a plain-English
       explanation.
    6. Drop fully-empty rows and remove exact cross-file duplicates.
    7. Tag every row with source_file and source_row before any rows are
       removed, so quarantine messages can say "row 23 of Q3_sales.xlsx".
    8. Return one merged DataFrame and a list of CleaningEntry records
       suitable for insertion into the cleaning_log SQLite table.

Public functions:

    consolidate(folder_path): Discover and merge all files in a folder.
    load_file(file_path, log): Read and clean a single file.
    standardize_columns(df, source_file, log): Map column name variants.
    normalize_dates(df, source_file, log): Parse mixed date formats.
    clean_numeric_columns(df, source_file, log): Strip currency formatting.
    remove_duplicates(df, log): Remove exact cross-file duplicates.
    handle_missing_values(df, source_file, log): Drop or flag empty rows.
    tag_source(df, file_path, header_row_idx): Add source metadata columns.
    log_action(log, source_file, transformation, original_value, new_value):
        Append a CleaningEntry to the log.
    detect_header_row(raw_df): Find the true header row index.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


# ── Canonical schema ──────────────────────────────────────────────────────

CANONICAL_COLUMNS: frozenset[str] = frozenset(
    {"date", "product", "region", "sales_rep", "customer", "quantity", "revenue"}
)

# Maps every known column-name variant (lowercased + stripped) → canonical name.
# Add entries here to support new file formats with no Python changes required.
COLUMN_MAP: dict[str, str] = {
    # date
    "date": "date",
    "transaction_date": "date",
    "sale date": "date",
    "transaction date": "date",
    "return date": "date",
    # product
    "product": "product",
    "product_name": "product",
    "item": "product",
    "product line": "product",
    "product name": "product",
    # region
    "region": "region",
    "territory": "region",
    "area": "region",
    # sales_rep
    "sales rep": "sales_rep",
    "rep": "sales_rep",
    "salesperson": "sales_rep",
    "rep name": "sales_rep",
    "sales_rep": "sales_rep",
    # customer
    "customer": "customer",
    "client": "customer",
    "account": "customer",
    "client name": "customer",
    # quantity
    "qty": "quantity",
    "units": "quantity",
    "quantity": "quantity",
    "units sold": "quantity",
    # revenue
    "revenue": "revenue",
    "rev.": "revenue",
    "total revenue": "revenue",
    "revenue ($)": "revenue",
    "$": "revenue",
    "amount": "revenue",
    "refund amt": "revenue",
}

# Columns treated as numeric; cleaned by clean_numeric_columns().
_NUMERIC_COLUMNS: tuple[str, ...] = ("quantity", "revenue")

# Minimum number of COLUMN_MAP matches needed to declare a row the header.
_HEADER_DETECTION_THRESHOLD: int = 3
# How many rows to scan before giving up on header detection.
_MAX_HEADER_SCAN_ROWS: int = 10


# ── Cleaning log record ───────────────────────────────────────────────────

@dataclass
class CleaningEntry:
    """One transformation record destined for the cleaning_log SQLite table.

    Attributes:
        source_file:    Name of the file the transformation was applied to.
                        Use "(all files)" for cross-file operations.
        transformation: Short snake_case label — e.g. "rename_column",
                        "normalize_date", "strip_currency", "drop_empty_rows".
        original_value: What was present before the change, as a human-readable
                        string — e.g. the original column name or raw cell value.
        new_value:      What replaced it, or a summary of the action taken.
        timestamp:      ISO-8601 UTC timestamp recorded at entry creation.
    """

    source_file: str
    transformation: str
    original_value: str
    new_value: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ── Public API ────────────────────────────────────────────────────────────

def consolidate(folder_path: str | Path) -> tuple[pd.DataFrame, list[CleaningEntry]]:
    """Accept a folder path, discover all .xlsx / .xls / .csv files, and
    return a single merged DataFrame alongside a full cleaning log.

    Iterates over discovered files in sorted order.  Each file is processed
    by load_file(), which applies column standardization, date normalization,
    numeric cleaning, source tagging, and empty-row removal.  After all files
    are loaded the DataFrames are concatenated and exact cross-file duplicates
    are removed via remove_duplicates().

    Files whose names start with "~$" (Excel auto-save lock files) are
    silently skipped.

    Args:
        folder_path: Path to a local directory containing spreadsheet files.

    Returns:
        A tuple of:
          - merged_df: DataFrame with canonical column names plus source_file
            and source_row on every row.
          - cleaning_log: Flat list of CleaningEntry records documenting every
            structural transformation applied across all files.

    Raises:
        FileNotFoundError: If the folder contains no supported files.
    """
    folder = Path(folder_path)
    files = sorted(
        f for f in folder.iterdir()
        if f.suffix.lower() in {".xlsx", ".xls", ".csv"}
        and not f.name.startswith("~$")
    )

    if not files:
        raise FileNotFoundError(f"No Excel/CSV files found in: {folder_path}")

    log: list[CleaningEntry] = []
    frames: list[pd.DataFrame] = []

    for file_path in files:
        df = load_file(file_path, log)
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True)
    merged = remove_duplicates(merged, log)
    return merged, log


def load_file(file_path: str | Path, log: list[CleaningEntry]) -> pd.DataFrame:
    """Read a single Excel or CSV file and return a standardized DataFrame.

    Orchestrates the full per-file cleaning pipeline:
      1. Read raw contents with no header parsing (all cells as strings).
      2. Detect the true header row — skipping any title or spacer rows above
         it (e.g. west_region_2024.xlsx has two such rows).
      3. Promote the detected row to column names and discard rows above it.
      4. Standardize column names to canonical form.
      5. Inject "West" into the region column if it is absent and the
         filename contains "west" (west_region_2024.xlsx carries no region
         column because all rows are West-region by definition).
      6. Tag every row with source_file and source_row before any rows are
         dropped, so the numbers remain accurate for quarantine messages.
      7. Normalize date strings to YYYY-MM-DD.
      8. Clean numeric columns (strip "$", commas, whitespace).
      9. Drop fully-empty rows.

    All transformations are appended to the shared log via log_action().

    Args:
        file_path: Path to a single .xlsx, .xls, or .csv file.
        log:       Shared cleaning log list; entries are appended in place.

    Returns:
        DataFrame with canonical column names plus source_file / source_row.

    Raises:
        ValueError: If the file extension is not .xlsx, .xls, or .csv.
    """
    file_path = Path(file_path)
    source = file_path.name

    raw_df = _read_raw(file_path)
    header_idx = detect_header_row(raw_df)

    if header_idx > 0:
        log_action(
            log, source, "skip_title_rows",
            f"rows 0-{header_idx - 1} above header",
            f"header detected at row {header_idx}",
        )

    df = _promote_header(raw_df, header_idx)
    df = standardize_columns(df, source, log)

    if "region" not in df.columns and "west" in source.lower():
        df["region"] = "West"
        log_action(log, source, "inject_region", "(column absent)", "West")

    df = tag_source(df, file_path, header_idx)
    df = normalize_dates(df, source, log)
    df = clean_numeric_columns(df, source, log)
    df = handle_missing_values(df, source, log)
    return df


def standardize_columns(
    df: pd.DataFrame,
    source_file: str,
    log: list[CleaningEntry],
) -> pd.DataFrame:
    """Standardize column names by mapping known variants to canonical names.

    Iterates over each column name, lowercases and strips it, and looks it up
    in COLUMN_MAP.  On a match, the column is renamed and the change is
    appended to the log.  Columns with no mapping are left unchanged — they
    will appear as unknown fields and surface through the validator.

    Args:
        df:          DataFrame with the file's original column names.
        source_file: Filename used for CleaningEntry records.
        log:         Shared cleaning log list; entries are appended in place.

    Returns:
        DataFrame with canonical column names where mappings exist.
    """
    rename_map: dict[str, str] = {}

    for col in df.columns:
        if not isinstance(col, str):
            continue
        canonical = COLUMN_MAP.get(col.strip().lower())
        if canonical is not None and canonical != col:
            rename_map[col] = canonical
            log_action(log, source_file, "rename_column", col, canonical)

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def normalize_dates(
    df: pd.DataFrame,
    source_file: str,
    log: list[CleaningEntry],
) -> pd.DataFrame:
    """Normalize the date column to uniform YYYY-MM-DD strings.

    Attempts to parse every value in the "date" column using pandas
    mixed-format inference, which handles:
      - ISO 8601:       "2024-03-15"
      - US locale:      "03/15/2024"
      - Written full:   "March 15, 2024"
      - Written short:  "Mar 15, 2024"

    Values that cannot be parsed are left as-is (NaT becomes the original
    string).  The validator will catch these and quarantine them.  Successfully
    parsed values are converted to "YYYY-MM-DD" strings.

    Does nothing if the DataFrame has no "date" column.

    Args:
        df:          DataFrame containing a "date" column (optional).
        source_file: Filename used for CleaningEntry records.
        log:         Shared cleaning log list; entries are appended in place.

    Returns:
        DataFrame with the "date" column normalized where parseable.
    """
    if "date" not in df.columns:
        return df

    original = df["date"].copy()
    parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=False, errors="coerce")

    # Replace successfully parsed values with ISO strings; keep originals for
    # values that failed so the validator can write a meaningful error message.
    df["date"] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), original)

    n_converted = int(parsed.notna().sum())
    n_failed = int(parsed.isna().sum() - original.isna().sum())  # newly-failed only

    if n_converted:
        log_action(
            log, source_file, "normalize_date",
            "mixed date formats",
            f"{n_converted} value(s) converted to YYYY-MM-DD",
        )
    if n_failed > 0:
        log_action(
            log, source_file, "normalize_date_failed",
            f"{n_failed} unparseable date value(s)",
            "left unchanged for validator",
        )

    return df


def clean_numeric_columns(
    df: pd.DataFrame,
    source_file: str,
    log: list[CleaningEntry],
) -> pd.DataFrame:
    """Strip currency symbols, commas, and whitespace from numeric columns.

    Processes every column in _NUMERIC_COLUMNS ("quantity" and "revenue")
    that is present in df.  For each cell, _coerce_numeric() attempts to
    produce a clean numeric string.  If the value can be parsed as a float
    after cleaning, the clean version replaces the original.  If it cannot
    (e.g. "pending", "TBD", "N/A"), the original value is preserved so the
    validator can quarantine it with a meaningful message.

    Common patterns handled:
      - "$3,000"  → "3000.0"
      - "  1 500 " → "1500.0"  (some locales use space as thousands separator)
      - "-450.00"  → "-450.0"  (negatives preserved; validator checks sign rules)

    Args:
        df:          DataFrame potentially containing quantity / revenue columns.
        source_file: Filename used for CleaningEntry records.
        log:         Shared cleaning log list; entries are appended in place.

    Returns:
        DataFrame with numeric columns cleaned where possible.
    """
    for col in _NUMERIC_COLUMNS:
        if col not in df.columns:
            continue

        cleaned_series = df[col].apply(_coerce_numeric)
        changed_mask = (cleaned_series != df[col]) & cleaned_series.notna()
        n_cleaned = int(changed_mask.sum())

        if n_cleaned:
            df[col] = cleaned_series
            log_action(
                log, source_file, "strip_currency_symbols",
                f"{n_cleaned} value(s) in '{col}' had symbols/commas",
                "stripped and stored as plain numeric string",
            )

    return df


def remove_duplicates(
    df: pd.DataFrame,
    log: list[CleaningEntry],
) -> pd.DataFrame:
    """Flag and remove exact duplicate rows across the merged DataFrame.

    Two rows are considered duplicates when every data column is identical.
    The source_file and source_row columns are excluded from the equality
    check because the same transaction appearing in two different files with
    different source labels should still be detected as a duplicate.

    The first occurrence is kept; subsequent occurrences are dropped.

    Args:
        df:  Merged DataFrame produced by concatenating all per-file frames.
        log: Shared cleaning log list; entries are appended in place.

    Returns:
        DataFrame with duplicate rows removed.
    """
    data_cols = [c for c in df.columns if c not in ("source_file", "source_row")]
    dups_mask = df.duplicated(subset=data_cols, keep="first")
    n_dups = int(dups_mask.sum())

    if n_dups:
        log_action(
            log, "(all files)", "remove_exact_duplicates",
            f"{n_dups} duplicate row(s) found across files",
            "removed — kept first occurrence per file sort order",
        )
        df = df[~dups_mask].reset_index(drop=True)

    return df


def handle_missing_values(
    df: pd.DataFrame,
    source_file: str,
    log: list[CleaningEntry],
    strategy: str = "drop_empty",
) -> pd.DataFrame:
    """Handle missing values in the DataFrame using the specified strategy.

    Two strategies are supported:

        ``drop_empty`` (default): Drop rows where every data column is null or
        blank. This catches entirely empty rows that Excel files sometimes
        contain between data blocks. Rows with partial data are preserved so
        the validator can quarantine them with a column-specific explanation.

        ``flag``: Instead of dropping, add a boolean ``_all_empty`` column
        marking the fully-empty rows. Useful for inspection; downstream steps
        can decide what to do.

    The ``source_file`` and ``source_row`` columns are excluded from the
    emptiness check (they are always populated after ``tag_source()``).

    Args:
        df:          DataFrame with source_file and source_row already added.
        source_file: Filename used for CleaningEntry records.
        log:         Shared cleaning log list; entries are appended in place.
        strategy:    One of "drop_empty" or "flag".

    Returns:
        DataFrame with missing values handled per the chosen strategy.

    Raises:
        ValueError: If an unrecognised strategy string is passed.
    """
    data_cols = [c for c in df.columns if c not in ("source_file", "source_row")]
    empty_mask = df[data_cols].replace("", pd.NA).isna().all(axis=1)
    n_empty = int(empty_mask.sum())

    if strategy == "drop_empty":
        if n_empty:
            log_action(
                log, source_file, "drop_empty_rows",
                f"{n_empty} fully-empty row(s)",
                "removed",
            )
            df = df[~empty_mask].reset_index(drop=True)

    elif strategy == "flag":
        df["_all_empty"] = empty_mask
        if n_empty:
            log_action(
                log, source_file, "flag_empty_rows",
                f"{n_empty} fully-empty row(s)",
                "flagged in '_all_empty' column",
            )

    else:
        raise ValueError(
            f"Unknown missing-value strategy '{strategy}'. "
            "Expected 'drop_empty' or 'flag'."
        )

    return df


def tag_source(
    df: pd.DataFrame,
    file_path: Path,
    header_row_idx: int,
) -> pd.DataFrame:
    """Output a DataFrame with source_file and source_row columns prepended.

    Must be called before any rows are dropped so that source_row values
    match the actual row numbers in the originating file.  This allows the
    validator to produce quarantine messages such as:
        "revenue is negative (-450) in row 23 of Q3_sales.xlsx"

    Row number calculation:
        Excel rows are 1-based.  The header occupies one row.  Any title rows
        above the header are indexed 1 … header_row_idx.  So the first data
        row has Excel row number = header_row_idx + 2.

    Args:
        df:             DataFrame after header promotion and column renaming,
                        but before any data rows are removed.
        file_path:      Path to the source file (used for the filename string).
        header_row_idx: 0-based index of the header row in the raw file.

    Returns:
        DataFrame with "source_file" and "source_row" inserted as the first
        two columns.
    """
    excel_offset = header_row_idx + 2  # +1 for 1-indexing, +1 for the header row
    df = df.copy()
    df.insert(0, "source_file", file_path.name)
    df.insert(1, "source_row", range(excel_offset, excel_offset + len(df)))
    return df


def log_action(
    log: list[CleaningEntry],
    source_file: str,
    transformation: str,
    original_value: str,
    new_value: str,
) -> None:
    """Append a single CleaningEntry to the shared cleaning log.

    Centralising entry creation here ensures every record has a consistent
    UTC timestamp and field layout.  All other functions in this module call
    log_action() rather than constructing CleaningEntry objects directly.

    Args:
        log:            The cleaning log list to append to.
        source_file:    Filename the transformation applies to.  Use
                        "(all files)" for cross-file operations.
        transformation: Short snake_case label describing the action.
        original_value: Human-readable description of the pre-change state.
        new_value:      Human-readable description of the post-change state
                        or the action taken.
    """
    log.append(CleaningEntry(
        source_file=source_file,
        transformation=transformation,
        original_value=original_value,
        new_value=new_value,
    ))


def detect_header_row(raw_df: pd.DataFrame) -> int:
    """Return the 0-based index of the row that contains the true column headers.

    Scans the first _MAX_HEADER_SCAN_ROWS rows.  A row is declared the header
    when it contains at least _HEADER_DETECTION_THRESHOLD cell values that
    match a known column-name variant in COLUMN_MAP (case-insensitive, after
    stripping whitespace).

    Falls back to 0 if no row meets the threshold — the correct behaviour for
    well-formed files where the header is already the first row.

    Args:
        raw_df: DataFrame read with header=None so all row indices are ints.

    Returns:
        0-based row index of the header row.
    """
    n_rows = min(_MAX_HEADER_SCAN_ROWS, len(raw_df))
    for i in range(n_rows):
        row_values = raw_df.iloc[i].dropna().astype(str)
        matches = sum(
            1 for v in row_values
            if v.strip().lower() in COLUMN_MAP
        )
        if matches >= _HEADER_DETECTION_THRESHOLD:
            return i
    return 0


# ── Private helpers ───────────────────────────────────────────────────────

def _read_raw(file_path: Path) -> pd.DataFrame:
    """Read a spreadsheet file with no header parsing, all cells as strings.

    Reading with header=None and dtype=str prevents pandas from coercing
    date strings, floats, or None values before the consolidator has had a
    chance to inspect raw cell content for header detection.

    Args:
        file_path: Path to an .xlsx, .xls, or .csv file.

    Returns:
        DataFrame with integer column labels (0, 1, 2, …) where row 0 may
        be the true header, a title row, or a blank spacer.

    Raises:
        ValueError: If the file extension is not .xlsx, .xls, or .csv.
    """
    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, header=None, dtype=str)
    if suffix == ".csv":
        return pd.read_csv(file_path, header=None, dtype=str)
    raise ValueError(
        f"Unsupported file type '{suffix}'. Expected .xlsx, .xls, or .csv."
    )


def _promote_header(raw_df: pd.DataFrame, header_row_idx: int) -> pd.DataFrame:
    """Promote a detected row to column names and drop all rows above it.

    Args:
        raw_df:         DataFrame with integer column labels (header=None).
        header_row_idx: 0-based index of the row that holds the column names.

    Returns:
        DataFrame with string column names from header_row_idx and data rows
        starting at index 0.
    """
    header: list[str] = raw_df.iloc[header_row_idx].tolist()
    df = raw_df.iloc[header_row_idx + 1:].copy()
    df.columns = pd.Index(header)
    return df.reset_index(drop=True)


def _coerce_numeric(val: Any) -> Any:
    """Strip currency formatting from a single cell value and return a clean
    numeric string, or return the original value unchanged if it cannot be
    parsed as a number.

    Handles:
      - "$3,000"   → "3000.0"
      - " 1,500.5" → "1500.5"
      - "-450.00"  → "-450.0"  (sign preserved)
      - "pending"  → "pending" (unchanged; validator will quarantine)
      - None / NaN → returned as-is

    Args:
        val: A raw cell value, typically a string or None.

    Returns:
        A plain numeric string if the value is numeric after cleaning, or
        the original value unchanged.
    """
    if val is None:
        return val
    if isinstance(val, float) and pd.isna(val):
        return val

    cleaned = re.sub(r"[$,\s]", "", str(val).strip())
    try:
        numeric = float(cleaned)
        return str(numeric)
    except ValueError:
        return val
