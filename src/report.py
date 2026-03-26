"""Cleaning and quarantine summary report for the Excel consolidation pipeline.

Reads from the three pipeline tables (``consolidated``, ``quarantine``,
``cleaning_log``) produced by ``db_loader.py`` and generates two things:

    1. A **cleaning summary** — files processed, rows before/after validation,
       columns standardised, duplicates removed, and type/format fixes applied.
    2. A **quarantine summary** — total quarantined rows, breakdown by failure
       category, and breakdown by source file.

Two output formats are supported:

    - Terminal text (always produced; returned as a string and printed).
    - Markdown report file (written to a caller-supplied path when requested).

Design — functional style::

        db_path ──► read_consolidated  ──┐
                                         ├──► generate_cleaning_summary  ──┐
        db_path ──► read_quarantine    ──┤                                 ├──► render_terminal
                                         ├──► generate_quarantine_summary ─┤
        db_path ──► read_cleaning_log  ──┘                                 └──► render_markdown

Every public function is pure: given the same inputs it always returns the
same outputs.  The only side effects are printing (``render_terminal``) and
writing a single file (``render_markdown``).

Public functions:

    read_consolidated(db_path):
        Load the ``consolidated`` table from SQLite into a DataFrame.
    read_quarantine(db_path):
        Load the ``quarantine`` table from SQLite into a DataFrame.
    read_cleaning_log(db_path):
        Load the ``cleaning_log`` table from SQLite into a DataFrame.
    generate_cleaning_summary(clean_df, quarantine_df, log_df):
        Bullet 1 — compute files processed, rows before/after, columns
        standardised, duplicates removed, and type/format fixes applied.
    generate_quarantine_summary(quarantine_df):
        Bullet 2 — compute total quarantined count, breakdown by reason
        category, and breakdown by source file.
    render_terminal(cleaning, quarantine):
        Bullet 3a — format both summaries as plain text, print, and return.
    render_markdown(cleaning, quarantine, output_path):
        Bullet 3b — write both summaries as a Markdown file, return path.
    report(db_path, output_path, fmt):
        Orchestrate the full pipeline in one call (reads DB, computes
        summaries, renders terminal output, optionally writes a file).
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — transformation labels emitted by consolidator.py
# ---------------------------------------------------------------------------

_RENAME_COLUMN: str = "rename_column"
_REMOVE_DUPLICATES: str = "remove_exact_duplicates"
_NORMALIZE_DATE: str = "normalize_date"
_STRIP_CURRENCY: str = "strip_currency_symbols"

# Columns added by SQLite (id, loaded_at / quarantined_at) that carry no
# meaning in a report.
_DROP_COLS: frozenset[str] = frozenset({"id", "loaded_at", "quarantined_at"})


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class CleaningSummary:
    """Computed metrics about the structural-cleaning pass.

    Attributes:
        n_files:                Number of distinct source files processed.
        file_names:             Sorted list of source file basenames.
        n_rows_before:          Total rows entering the validation step
                                (clean + quarantined combined).
        n_rows_after:           Rows that passed validation and were loaded
                                into the ``consolidated`` table.
        n_columns_standardized: Number of column-rename operations applied
                                across all files (one per alias resolved).
        n_duplicates_removed:   Exact-duplicate rows dropped across files.
        n_type_fixes:           Sum of date normalisation conversions and
                                currency-strip operations applied.
        transformation_counts:  Full breakdown of every transformation type
                                recorded in the cleaning log, mapping the
                                transformation label to its occurrence count.
    """

    n_files: int
    file_names: list[str]
    n_rows_before: int
    n_rows_after: int
    n_columns_standardized: int
    n_duplicates_removed: int
    n_type_fixes: int
    transformation_counts: dict[str, int]


@dataclass
class QuarantineSummary:
    """Computed metrics about rows that failed validation.

    Attributes:
        n_quarantined:  Total number of quarantined rows.
        by_reason_type: Mapping from failure-category label to row count.
                        Categories are derived by keyword-matching the
                        plain-English ``quarantine_reason`` strings written
                        by ``validator.py``.
        by_source_file: Mapping from source filename to quarantined-row count.
    """

    n_quarantined: int
    by_reason_type: dict[str, int] = field(default_factory=dict)
    by_source_file: dict[str, int] = field(default_factory=dict)


@dataclass
class ReportResult:
    """Output produced by the top-level ``report()`` orchestrator.

    Attributes:
        terminal_text: The plain-text report that was printed to stdout.
        output_path:   Path to the written report file, or ``None`` when no
                       file output was requested.
    """

    terminal_text: str
    output_path: Optional[Path]


# ---------------------------------------------------------------------------
# DB readers — one per pipeline table
# ---------------------------------------------------------------------------


def read_consolidated(db_path: str | Path) -> pd.DataFrame:
    """Load the ``consolidated`` table from a SQLite database.

    Args:
        db_path: Path to the SQLite file produced by ``db_loader.load()``.

    Returns:
        DataFrame of all clean rows, with internal database columns removed.

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

    Args:
        db_path: Path to the SQLite file produced by ``db_loader.load()``.

    Returns:
        DataFrame of all quarantined rows, with internal database columns
        removed.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM quarantine", conn)
    return _drop_db_cols(df)


def read_cleaning_log(db_path: str | Path) -> pd.DataFrame:
    """Load the ``cleaning_log`` table from a SQLite database.

    Args:
        db_path: Path to the SQLite file produced by ``db_loader.load()``.

    Returns:
        DataFrame with columns ``source_file``, ``transformation``,
        ``original_value``, ``new_value``, ``timestamp``.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM cleaning_log", conn)
    return _drop_db_cols(df)


# ---------------------------------------------------------------------------
# Bullet 1 — cleaning summary
# ---------------------------------------------------------------------------


def generate_cleaning_summary(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
    log_df: pd.DataFrame,
) -> CleaningSummary:
    """Compute cleaning metrics from the post-validation DataFrames and log.

    Derives every metric from the data rather than relying on pre-computed
    counts, so the summary always reflects the actual database state.

    Args:
        clean_df:      Rows from the ``consolidated`` table (passed validation).
        quarantine_df: Rows from the ``quarantine`` table (failed validation).
        log_df:        Rows from the ``cleaning_log`` table (one row per
                       structural transformation applied during consolidation).

    Returns:
        ``CleaningSummary`` with file counts, row counts, column rename count,
        duplicate count, type-fix count, and a full transformation breakdown.
    """
    file_names = _collect_file_names(clean_df, quarantine_df)

    n_columns_standardized = _count_transformations(log_df, _RENAME_COLUMN)

    n_duplicates_removed = _sum_leading_ints(
        log_df, _REMOVE_DUPLICATES, column="original_value"
    )

    n_date_fixes = _sum_leading_ints(
        log_df, _NORMALIZE_DATE, column="new_value"
    )
    n_currency_fixes = _sum_leading_ints(
        log_df, _STRIP_CURRENCY, column="original_value"
    )

    transformation_counts: dict[str, int] = (
        log_df.groupby("transformation").size().to_dict()
        if not log_df.empty and "transformation" in log_df.columns
        else {}
    )

    return CleaningSummary(
        n_files=len(file_names),
        file_names=file_names,
        n_rows_before=len(clean_df) + len(quarantine_df),
        n_rows_after=len(clean_df),
        n_columns_standardized=n_columns_standardized,
        n_duplicates_removed=n_duplicates_removed,
        n_type_fixes=n_date_fixes + n_currency_fixes,
        transformation_counts=transformation_counts,
    )


# ---------------------------------------------------------------------------
# Bullet 2 — quarantine summary
# ---------------------------------------------------------------------------


def generate_quarantine_summary(quarantine_df: pd.DataFrame) -> QuarantineSummary:
    """Compute quarantine metrics from the quarantined rows DataFrame.

    Parses the plain-English ``quarantine_reason`` strings written by
    ``validator.py`` to group failures into human-readable categories.
    Each reason string may contain multiple semicolon-separated clauses;
    every clause is classified independently so a row with two failures
    contributes one count to each relevant category.

    Args:
        quarantine_df: Rows from the ``quarantine`` table, including a
                       ``quarantine_reason`` column and a ``source_file``
                       column.

    Returns:
        ``QuarantineSummary`` with total count, reason-type breakdown, and
        per-file breakdown.
    """
    if quarantine_df.empty:
        return QuarantineSummary(n_quarantined=0)

    by_source_file: dict[str, int] = (
        quarantine_df.groupby("source_file").size().to_dict()
        if "source_file" in quarantine_df.columns
        else {}
    )

    by_reason_type: dict[str, int] = {}
    if "quarantine_reason" in quarantine_df.columns:
        for reason_string in quarantine_df["quarantine_reason"].dropna():
            for clause in str(reason_string).split("; "):
                clause = clause.strip()
                if not clause:
                    continue
                category = _classify_reason(clause)
                by_reason_type[category] = by_reason_type.get(category, 0) + 1

    return QuarantineSummary(
        n_quarantined=len(quarantine_df),
        by_reason_type=by_reason_type,
        by_source_file=by_source_file,
    )


# ---------------------------------------------------------------------------
# Bullet 3a — terminal output
# ---------------------------------------------------------------------------


def render_terminal(
    cleaning: CleaningSummary,
    quarantine: QuarantineSummary,
) -> str:
    """Format both summaries as a plain-text block, print it, and return it.

    Note:
        This function has a side effect: it prints the formatted report to
        stdout via ``print()``.

    Args:
        cleaning:   ``CleaningSummary`` produced by ``generate_cleaning_summary``.
        quarantine: ``QuarantineSummary`` produced by ``generate_quarantine_summary``.

    Returns:
        The formatted report string (identical to what was printed).
    """
    lines: list[str] = []

    lines.append("=" * 60)
    lines.append("CONSOLIDATION REPORT")
    lines.append("=" * 60)

    # -- Cleaning summary --------------------------------------------------
    lines.append("")
    lines.append("CLEANING SUMMARY")
    lines.append("-" * 40)
    lines.append(f"  Files processed:          {cleaning.n_files}")
    for fname in cleaning.file_names:
        lines.append(f"    - {fname}")
    lines.append(f"  Total rows (before valid): {cleaning.n_rows_before}")
    lines.append(f"  Clean rows loaded:         {cleaning.n_rows_after}")
    lines.append(f"  Rows quarantined:          {quarantine.n_quarantined}")
    lines.append(f"  Columns standardized:      {cleaning.n_columns_standardized}")
    lines.append(f"  Duplicate rows removed:    {cleaning.n_duplicates_removed}")
    lines.append(f"  Type/format fixes applied: {cleaning.n_type_fixes}")

    if cleaning.transformation_counts:
        lines.append("")
        lines.append("  Transformation breakdown:")
        for label, count in sorted(cleaning.transformation_counts.items()):
            lines.append(f"    {label:<35} {count}")

    # -- Quarantine summary ------------------------------------------------
    lines.append("")
    lines.append("QUARANTINE SUMMARY")
    lines.append("-" * 40)

    if quarantine.n_quarantined == 0:
        lines.append("  No rows quarantined.")
    else:
        lines.append(f"  Total quarantined: {quarantine.n_quarantined} row(s)")

        if quarantine.by_reason_type:
            lines.append("")
            lines.append("  By failure category:")
            for category, count in sorted(
                quarantine.by_reason_type.items(), key=lambda kv: -kv[1]
            ):
                lines.append(f"    {category:<35} {count}")

        if quarantine.by_source_file:
            lines.append("")
            lines.append("  By source file:")
            for fname, count in sorted(quarantine.by_source_file.items()):
                lines.append(f"    {fname:<35} {count}")

    lines.append("")
    lines.append("=" * 60)

    text = "\n".join(lines)
    print(text)
    return text


# ---------------------------------------------------------------------------
# Bullet 3b — markdown file output
# ---------------------------------------------------------------------------


def render_markdown(
    cleaning: CleaningSummary,
    quarantine: QuarantineSummary,
    output_path: str | Path,
) -> Path:
    """Write both summaries as a Markdown report file and return the path.

    The file uses standard Markdown with headers, bullet lists, and tables.
    The parent directory is created automatically if absent.

    Note:
        This function has a side effect: it writes a file at ``output_path``.

    Args:
        cleaning:     ``CleaningSummary`` produced by ``generate_cleaning_summary``.
        quarantine:   ``QuarantineSummary`` produced by ``generate_quarantine_summary``.
        output_path:  Path where the ``.md`` file should be written.

    Returns:
        The resolved ``Path`` to the written file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []

    lines.append("# Consolidation Report")
    lines.append("")

    # -- Cleaning summary --------------------------------------------------
    lines.append("## Cleaning Summary")
    lines.append("")
    lines.append(f"- **Files processed:** {cleaning.n_files}")
    for fname in cleaning.file_names:
        lines.append(f"  - {fname}")
    lines.append(f"- **Total rows (before validation):** {cleaning.n_rows_before}")
    lines.append(f"- **Clean rows loaded:** {cleaning.n_rows_after}")
    lines.append(f"- **Rows quarantined:** {quarantine.n_quarantined}")
    lines.append(f"- **Columns standardized:** {cleaning.n_columns_standardized}")
    lines.append(f"- **Duplicate rows removed:** {cleaning.n_duplicates_removed}")
    lines.append(f"- **Type/format fixes applied:** {cleaning.n_type_fixes}")
    lines.append("")

    if cleaning.transformation_counts:
        lines.append("### Transformation Breakdown")
        lines.append("")
        lines.append("| Transformation | Count |")
        lines.append("|---|---|")
        for label, count in sorted(cleaning.transformation_counts.items()):
            lines.append(f"| {label} | {count} |")
        lines.append("")

    # -- Quarantine summary ------------------------------------------------
    lines.append("## Quarantine Summary")
    lines.append("")

    if quarantine.n_quarantined == 0:
        lines.append("No rows were quarantined.")
        lines.append("")
    else:
        lines.append(
            f"**{quarantine.n_quarantined} row(s) quarantined** across "
            f"{cleaning.n_files} file(s)."
        )
        lines.append("")

        if quarantine.by_reason_type:
            lines.append("### By Failure Category")
            lines.append("")
            lines.append("| Reason | Count |")
            lines.append("|---|---|")
            for category, count in sorted(
                quarantine.by_reason_type.items(), key=lambda kv: -kv[1]
            ):
                lines.append(f"| {category} | {count} |")
            lines.append("")

        if quarantine.by_source_file:
            lines.append("### By Source File")
            lines.append("")
            lines.append("| File | Quarantined Rows |")
            lines.append("|---|---|")
            for fname, count in sorted(quarantine.by_source_file.items()):
                lines.append(f"| {fname} | {count} |")
            lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to: %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def report(
    db_path: str | Path,
    output_path: Optional[str | Path] = None,
    fmt: str = "markdown",
) -> ReportResult:
    """Run the full reporting pipeline against a pipeline SQLite database.

    Reads the three pipeline tables, computes both summaries, prints the
    terminal report, and optionally writes a file in the requested format.

    Args:
        db_path:     Path to the SQLite file produced by ``db_loader.load()``.
        output_path: Path where the report file should be written.  Pass
                     ``None`` to skip file output and print only.
        fmt:         Output file format.  Currently ``"markdown"`` is the
                     only supported value.

    Returns:
        ``ReportResult`` containing the printed text and the written file
        path (or ``None`` if no file was requested).

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
        ValueError:        If ``fmt`` is not a recognised format string.
    """
    if fmt not in {"markdown"}:
        raise ValueError(f"Unsupported format '{fmt}'. Expected 'markdown'.")

    clean_df = read_consolidated(db_path)
    quarantine_df = read_quarantine(db_path)
    log_df = read_cleaning_log(db_path)

    cleaning = generate_cleaning_summary(clean_df, quarantine_df, log_df)
    quarantine = generate_quarantine_summary(quarantine_df)

    terminal_text = render_terminal(cleaning, quarantine)

    written_path: Optional[Path] = None
    if output_path is not None:
        if fmt == "markdown":
            written_path = render_markdown(cleaning, quarantine, output_path)

    return ReportResult(terminal_text=terminal_text, output_path=written_path)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _drop_db_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Remove internal SQLite columns from a freshly-read DataFrame.

    Args:
        df: DataFrame as returned by ``pd.read_sql``.

    Returns:
        New DataFrame with ``id``, ``loaded_at``, and ``quarantined_at``
        columns dropped if present.
    """
    cols_to_drop = [c for c in df.columns if c in _DROP_COLS]
    return df.drop(columns=cols_to_drop) if cols_to_drop else df


def _collect_file_names(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
) -> list[str]:
    """Return a sorted list of unique source file basenames across both tables.

    Args:
        clean_df:      Consolidated (clean) DataFrame.
        quarantine_df: Quarantined rows DataFrame.

    Returns:
        Sorted list of distinct ``source_file`` values found in either
        DataFrame.  Returns an empty list when neither DataFrame has a
        ``source_file`` column.
    """
    names: set[str] = set()
    for df in (clean_df, quarantine_df):
        if "source_file" in df.columns:
            names.update(df["source_file"].dropna().unique())
    return sorted(names)


def _count_transformations(log_df: pd.DataFrame, transformation: str) -> int:
    """Count rows in the cleaning log that match a given transformation label.

    Args:
        log_df:         Cleaning log DataFrame.
        transformation: Transformation label to filter on (e.g.
                        ``"rename_column"``).

    Returns:
        Number of matching rows, or 0 if the log is empty.
    """
    if log_df.empty or "transformation" not in log_df.columns:
        return 0
    return int((log_df["transformation"] == transformation).sum())


def _sum_leading_ints(
    log_df: pd.DataFrame,
    transformation: str,
    column: str,
) -> int:
    """Sum the leading integers from a log column for a specific transformation.

    Many cleaning log entries encode the count of affected rows as the first
    integer in their ``original_value`` or ``new_value`` text (e.g.
    ``"14 value(s) converted to YYYY-MM-DD"`` → 14).  This function extracts
    and sums those integers across all matching rows.

    Args:
        log_df:         Cleaning log DataFrame.
        transformation: Transformation label to filter on.
        column:         Name of the text column to parse (``"original_value"``
                        or ``"new_value"``).

    Returns:
        Sum of all leading integers found, or 0 if no matches or no integers.
    """
    if log_df.empty or "transformation" not in log_df.columns:
        return 0
    mask = log_df["transformation"] == transformation
    if not mask.any():
        return 0
    return int(log_df.loc[mask, column].apply(_parse_leading_int).sum())


def _parse_leading_int(text: str) -> int:
    """Extract the first integer from a text string.

    Args:
        text: String that may begin with an integer, e.g.
              ``"14 value(s) converted to YYYY-MM-DD"``.

    Returns:
        The first integer found, or 0 if no integer is present.

    Example:
        >>> _parse_leading_int("14 value(s) converted")
        14
        >>> _parse_leading_int("stripped and stored")
        0
    """
    match = re.search(r"\d+", str(text))
    return int(match.group()) if match else 0


def _classify_reason(clause: str) -> str:
    """Map a single quarantine-reason clause to a human-readable category.

    Matches against the exact message templates produced by ``validator.py``.
    The checks are ordered from most specific to least specific so that
    ``"is negative"`` (a specific range failure) is caught before generic
    fallbacks.

    Args:
        clause: One semicolon-delimited clause from a ``quarantine_reason``
                string, e.g. ``"revenue is negative (-450) in row 23 of ..."``.

    Returns:
        Category label string, one of:
        ``"negative value"``, ``"missing required field"``, ``"invalid date"``,
        ``"type mismatch"``, ``"out of range"``, ``"pattern mismatch"``,
        ``"sparse row"``, or ``"other"``.
    """
    lc = clause.lower()

    if "required field" in lc:
        return "missing required field"
    if "is not a valid number" in lc:
        return "type mismatch"
    if "is not a valid date" in lc:
        return "invalid date"
    if "is negative" in lc:
        return "negative value"
    if "is below minimum" in lc or "exceeds maximum" in lc:
        return "out of range"
    if "is before minimum" in lc or "is after maximum" in lc:
        return "out of range"
    if "does not match" in lc:
        return "pattern mismatch"
    if "row has only" in lc:
        return "sparse row"
    return "other"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured ``ArgumentParser`` for the report CLI.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Generate a cleaning and quarantine summary report from a "
            "pipeline SQLite database."
        )
    )
    parser.add_argument(
        "--db",
        default="data/seed.db",
        help="Path to the SQLite database file (default: data/seed.db).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Path for the report file output. "
            "Omit to print to terminal only."
        ),
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        default="markdown",
        choices=["markdown"],
        help="Output file format (default: markdown).",
    )
    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _build_arg_parser().parse_args()
    result = report(
        db_path=args.db,
        output_path=args.output,
        fmt=args.fmt,
    )
    if result.output_path is not None:
        print(f"\nReport written to: {result.output_path}")
