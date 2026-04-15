"""Export module for the Excel consolidation pipeline.

Reads clean and quarantined rows from the SQLite database produced by
``db_loader.py`` and writes them to a formatted Excel workbook.

Design — functional style:
    Every public function is pure: given the same inputs it returns the same
    outputs and has no observable side effects beyond writing a single file at
    the very end.  Data flows through a pipeline of transformations::

        db_path ──► read_consolidated ──┐
                                        ├──► build_sheet_map ──► write_workbook ──► ExportResult
        db_path ──► read_quarantine  ──┘

Public functions:

    read_consolidated(db_path):
        Load the ``consolidated`` table from SQLite into a DataFrame.
    read_quarantine(db_path):
        Load the ``quarantine`` table from SQLite into a DataFrame.
    split_by_column(df, col):
        Partition a DataFrame into per-value slices keyed by sheet-safe names.
    build_sheet_map(clean_df, quarantine_df, group_by):
        Assemble the ordered sheet-name → DataFrame mapping for the workbook.
    write_workbook(sheet_map, output_path):
        Write the sheet map to a formatted ``.xlsx`` file.
    build_summary(result):
        Return a plain-English summary string for an ExportResult.
    export(db_path, output_path, group_by):
        Orchestrate the full pipeline in one call.
"""

import argparse
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CLEAN_SHEET: str = "Clean Data"
_QUARANTINE_SHEET: str = "Quarantine"

# Columns present in SQLite tables that carry no meaning in a flat export.
_DROP_COLS: frozenset[str] = frozenset({"id"})

# Excel enforces a 31-character limit on sheet names.
_MAX_SHEET_NAME_LEN: int = 31

# Cap column widths so very long text cells don't make the file unusable.
_MAX_COL_WIDTH: int = 60


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ExportResult:
    """Summary of a completed Excel export operation.

    Attributes:
        output_path:  Path to the ``.xlsx`` workbook that was written.
        sheets:       Ordered list of sheet names written to the workbook.
        n_clean:      Total clean rows written across all non-quarantine sheets.
        n_quarantine: Total quarantined rows written to the quarantine sheet.
    """

    output_path: Path
    sheets: list[str]
    n_clean: int
    n_quarantine: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_consolidated(db_path: str | Path) -> pd.DataFrame:
    """Load the ``consolidated`` table from a SQLite database.

    The auto-generated ``id`` column is dropped — it is a database artefact
    with no meaning in a flat Excel export.

    Args:
        db_path: Path to the SQLite file produced by ``db_loader.load()``.

    Returns:
        DataFrame of all rows in the ``consolidated`` table, without the
        ``id`` column.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM consolidated", conn)
    return _drop_db_cols(df)


def read_quarantine(db_path: str | Path) -> pd.DataFrame:
    """Load the ``quarantine`` table from a SQLite database.

    The auto-generated ``id`` column is dropped — it is a database artefact
    with no meaning in a flat Excel export.

    Args:
        db_path: Path to the SQLite file produced by ``db_loader.load()``.

    Returns:
        DataFrame of all rows in the ``quarantine`` table, without the
        ``id`` column.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM quarantine", conn)
    return _drop_db_cols(df)


def split_by_column(df: pd.DataFrame, col: str) -> dict[str, pd.DataFrame]:
    """Partition a DataFrame into per-value slices keyed by sheet-safe names.

    Each unique value in ``col`` becomes a separate entry in the returned
    dict.  The key is a sheet-safe string (max 31 chars, per Excel's limit)
    derived from the column value.  Rows where ``col`` is null or blank are
    grouped under the key ``"(blank)"``.

    The grouping column is retained in each slice so the data remains
    self-describing when opened in Excel.

    Args:
        df:  DataFrame to partition.
        col: Column name to group by.

    Returns:
        Ordered dict mapping sheet-safe name → sub-DataFrame, sorted by
        the unique column values for deterministic sheet ordering.

    Raises:
        KeyError: If ``col`` is not present in ``df``.
    """
    if col not in df.columns:
        raise KeyError(
            f"Column '{col}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )

    fill_key = df[col].fillna("(blank)").replace("", "(blank)")
    return {
        _sheet_safe(value): group.reset_index(drop=True)
        for value, group in df.groupby(fill_key, sort=True)
    }


def build_sheet_map(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
    group_by: Optional[str] = None,
) -> dict[str, pd.DataFrame]:
    """Assemble the ordered sheet-name → DataFrame mapping for the workbook.

    When ``group_by`` is ``None`` the clean data lands on a single sheet
    named ``"Clean Data"``.  When ``group_by`` is supplied,
    ``split_by_column`` partitions the clean data and each slice gets a sheet
    prefixed with ``"Clean - "``.  The quarantine sheet is always appended
    last so it appears at the right-hand end of the tab bar.

    Args:
        clean_df:      DataFrame of clean rows from ``read_consolidated()``.
        quarantine_df: DataFrame of quarantined rows from ``read_quarantine()``.
        group_by:      Optional column name to split clean data into multiple
                       sheets (e.g. ``"region"`` or ``"source_file"``).
                       Pass ``None`` for a single ``"Clean Data"`` sheet.

    Returns:
        Ordered dict of sheet-name → DataFrame, suitable for passing
        directly to ``write_workbook()``.
    """
    if group_by is None:
        clean_sheets: dict[str, pd.DataFrame] = {_CLEAN_SHEET: clean_df}
    else:
        slices = split_by_column(clean_df, group_by)
        clean_sheets = {
            _sheet_safe(f"Clean - {name}"): frame
            for name, frame in slices.items()
        }

    return {**clean_sheets, _QUARANTINE_SHEET: quarantine_df}


def write_workbook(
    sheet_map: dict[str, pd.DataFrame],
    output_path: str | Path,
) -> ExportResult:
    """Write a sheet map to a formatted ``.xlsx`` workbook.

    Each entry in ``sheet_map`` becomes one worksheet in the order the dict
    was constructed.  Two formatting steps are applied per sheet:

    - Column widths are auto-sized to fit the widest value (capped at 60
      characters) so columns are immediately readable without manual resizing.
    - The first row is frozen so column headers stay visible while scrolling.

    Parent directories of ``output_path`` are created automatically if absent.

    Args:
        sheet_map:   Ordered dict of sheet-name → DataFrame, as produced by
                     ``build_sheet_map()``.
        output_path: Destination ``.xlsx`` file path.  Overwritten if it exists.

    Returns:
        ``ExportResult`` summarising the sheets and row counts written.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheets_written: list[str] = []
    n_clean: int = 0
    n_quarantine: int = 0

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            _autosize_columns(writer, sheet_name, df)
            _freeze_header(writer, sheet_name)
            sheets_written.append(sheet_name)

            if sheet_name == _QUARANTINE_SHEET:
                n_quarantine += len(df)
            else:
                n_clean += len(df)

    result = ExportResult(
        output_path=output_path,
        sheets=sheets_written,
        n_clean=n_clean,
        n_quarantine=n_quarantine,
    )
    logger.info(
        "Exported %d clean rows and %d quarantined rows → %s (%d sheet(s))",
        n_clean,
        n_quarantine,
        output_path,
        len(sheets_written),
    )
    return result


def build_summary(result: ExportResult) -> str:
    """Return a plain-English summary of a completed export operation.

    Args:
        result: ``ExportResult`` returned by ``write_workbook()``.

    Returns:
        Multi-line string, e.g.::

            Workbook written to: data/output/consolidated.xlsx
              Sheets: Clean Data, Quarantine
              347 clean rows, 12 quarantined rows
    """
    sheet_list = ", ".join(result.sheets)
    return (
        f"Workbook written to: {result.output_path}\n"
        f"  Sheets: {sheet_list}\n"
        f"  {result.n_clean} clean rows, {result.n_quarantine} quarantined rows"
    )


def export(
    db_path: str | Path,
    output_path: str | Path,
    group_by: Optional[str] = None,
) -> ExportResult:
    """Orchestrate the full export pipeline in a single call.

    Convenience wrapper that composes ``read_consolidated``,
    ``read_quarantine``, ``build_sheet_map``, and ``write_workbook`` into
    one function for callers that do not need access to the intermediate
    DataFrames.

    Args:
        db_path:     Path to the SQLite database produced by
                     ``db_loader.load()``.
        output_path: Destination ``.xlsx`` file path.
        group_by:    Optional column name to split clean data across multiple
                     sheets (e.g. ``"region"`` or ``"source_file"``).
                     Pass ``None`` for a single ``"Clean Data"`` sheet.

    Returns:
        ``ExportResult`` summarising the exported workbook.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
    """
    clean_df = read_consolidated(db_path)
    quarantine_df = read_quarantine(db_path)
    sheet_map = build_sheet_map(clean_df, quarantine_df, group_by)
    return write_workbook(sheet_map, output_path)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _drop_db_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Remove database-artefact columns from a DataFrame read from SQLite.

    Columns listed in ``_DROP_COLS`` that are absent from ``df`` are silently
    ignored, so this function is safe to call on any table.

    Args:
        df: DataFrame loaded directly from a SQLite table via ``pd.read_sql``.

    Returns:
        New DataFrame with every column in ``_DROP_COLS`` removed.
    """
    cols_to_drop = [c for c in _DROP_COLS if c in df.columns]
    return df.drop(columns=cols_to_drop) if cols_to_drop else df


def _sheet_safe(name: str) -> str:
    """Truncate ``name`` to the Excel sheet-name limit of 31 characters.

    Args:
        name: Candidate sheet name string.

    Returns:
        ``name`` truncated to at most 31 characters.
    """
    return str(name)[:_MAX_SHEET_NAME_LEN]


def _autosize_columns(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
) -> None:
    """Set each column's width to fit its widest value, capped at ``_MAX_COL_WIDTH``.

    Inspects both the column header string and every cell value to find the
    longest string representation, then sets ``column_dimensions.width`` on
    the openpyxl worksheet.  An extra two characters of padding are added so
    values don't sit flush against column borders.

    Args:
        writer:     Open ``pd.ExcelWriter`` whose engine is ``"openpyxl"``.
        sheet_name: Name of the sheet to resize.
        df:         DataFrame that was written to the sheet.
    """
    ws = writer.sheets[sheet_name]
    for col_idx, col_name in enumerate(df.columns, start=1):
        header_len = len(str(col_name))
        max_cell_len = (
            max(len(str(v)) for v in df[col_name].tolist())
            if not df.empty
            else 0
        )
        width = min(max(header_len, int(max_cell_len)) + 2, _MAX_COL_WIDTH)
        col_letter = ws.cell(row=1, column=col_idx).column_letter
        ws.column_dimensions[col_letter].width = width


def _freeze_header(writer: pd.ExcelWriter, sheet_name: str) -> None:
    """Freeze the first row of a worksheet so headers stay visible on scroll.

    Args:
        writer:     Open ``pd.ExcelWriter`` whose engine is ``"openpyxl"``.
        sheet_name: Name of the sheet to apply the freeze to.
    """
    writer.sheets[sheet_name].freeze_panes = "A2"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the standalone export script.

    Returns:
        Parsed namespace with ``db``, ``output``, and ``group_by`` attributes.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Export the consolidated and quarantine SQLite tables to a "
            "formatted Excel workbook."
        )
    )
    parser.add_argument(
        "--db",
        default="data/seed.db",
        help="Path to the SQLite database (default: data/seed.db).",
    )
    parser.add_argument(
        "--output",
        default="data/output/consolidated.xlsx",
        help=(
            "Destination .xlsx file path "
            "(default: data/output/consolidated.xlsx)."
        ),
    )
    parser.add_argument(
        "--group-by",
        dest="group_by",
        default=None,
        help=(
            "Split clean data into one sheet per unique value of this column "
            "(e.g. --group-by region). Omit for a single 'Clean Data' sheet."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    result = export(
        db_path=args.db,
        output_path=args.output,
        group_by=args.group_by,
    )
    print(build_summary(result))
