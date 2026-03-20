"""SQLite loading module for the Excel consolidation pipeline.

Accepts the outputs of the consolidation and validation steps and writes
them to a SQLite database.  Three tables are populated:

    consolidated  — clean rows that passed every validation rule.
    quarantine    — rows that failed at least one rule, with original data
                    preserved and a plain-English quarantine_reason column.
    cleaning_log  — every structural transformation applied during
                    consolidation (column renames, date normalisation, etc.).

Two load modes are supported:

    seed  (default) — drops and recreates all tables so the committed
          seed.db always represents a deterministic snapshot of the sample
          data.  Use this when regenerating data/seed.db from sample files.

    full  — appends rows to existing tables.  Use this for the live
          pipeline where rows accumulate over time.

Public functions:

    load(clean_df, quarantine_df, cleaning_log, db_path, mode):
        Orchestrate the full write: drop (seed mode), init schema, insert.
    resolve_db_path(base_dir, mode):
        Return the default SQLite path for a given mode.
    init_schema(conn):
        Create the three tables if they do not already exist.
    write_consolidated(conn, clean_df):
        Insert clean rows into the ``consolidated`` table.
    write_quarantine(conn, quarantine_df):
        Insert quarantined rows into the ``quarantine`` table.
    write_cleaning_log(conn, cleaning_log):
        Insert CleaningEntry records into the ``cleaning_log`` table.
    build_summary(result):
        Return a plain-English summary string for a LoadResult.
"""

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd

from consolidator import CleaningEntry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column layout constants
# ---------------------------------------------------------------------------

# Columns written to each table — must match _SCHEMA_SQL exactly.
# The auto-generated ``id`` and timestamp columns are intentionally absent:
# SQLite fills them via AUTOINCREMENT / DEFAULT so they must not appear in
# the DataFrame passed to to_sql().
_CONSOLIDATED_COLS: tuple[str, ...] = (
    "source_file",
    "source_row",
    "date",
    "product",
    "region",
    "sales_rep",
    "customer",
    "quantity",
    "revenue",
)

_QUARANTINE_COLS: tuple[str, ...] = (
    "quarantine_reason",
    "source_file",
    "source_row",
    "date",
    "product",
    "region",
    "sales_rep",
    "customer",
    "quantity",
    "revenue",
)

_LOG_COLS: tuple[str, ...] = (
    "source_file",
    "transformation",
    "original_value",
    "new_value",
    "timestamp",
)

_ALL_TABLES: tuple[str, ...] = ("consolidated", "quarantine", "cleaning_log")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class LoadResult:
    """Summary of a completed database load operation.

    Attributes:
        db_path:        Path to the SQLite database file that was written.
        n_consolidated: Number of clean rows written to ``consolidated``.
        n_quarantine:   Number of quarantined rows written to ``quarantine``.
        n_log_entries:  Number of entries written to ``cleaning_log``.
    """

    db_path: Path
    n_consolidated: int
    n_quarantine: int
    n_log_entries: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
    cleaning_log: list[CleaningEntry],
    db_path: str | Path,
    mode: str = "seed",
) -> LoadResult:
    """Load clean rows, quarantined rows, and the cleaning log into SQLite.

    In ``seed`` mode all existing pipeline tables are dropped and recreated
    from scratch so the committed ``seed.db`` always represents a
    deterministic, reproducible snapshot of the sample data.

    In ``full`` mode rows are appended to existing tables, allowing the
    live pipeline to accumulate rows across multiple runs.

    Parent directories of ``db_path`` are created automatically if absent.

    Args:
        clean_df:      DataFrame of rows that passed every validation rule,
                       as returned by ``validator.validate()``.
        quarantine_df: DataFrame of rows that failed at least one rule, with
                       a leading ``quarantine_reason`` column, as returned by
                       ``validator.validate()``.
        cleaning_log:  List of ``CleaningEntry`` records produced by
                       ``consolidator.consolidate()``.
        db_path:       Path to the target SQLite file.  Created if absent.
        mode:          ``"seed"`` (wipe and recreate) or ``"full"`` (append).

    Returns:
        ``LoadResult`` summarising how many rows were written to each table.

    Raises:
        ValueError: If ``mode`` is not ``"seed"`` or ``"full"``.
    """
    if mode not in {"seed", "full"}:
        raise ValueError(f"Unknown mode '{mode}'. Expected 'seed' or 'full'.")

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        if mode == "seed":
            _drop_tables(conn)
        init_schema(conn)
        n_consolidated = write_consolidated(conn, clean_df)
        n_quarantine = write_quarantine(conn, quarantine_df)
        n_log = write_cleaning_log(conn, cleaning_log)
        conn.commit()

    result = LoadResult(
        db_path=db_path,
        n_consolidated=n_consolidated,
        n_quarantine=n_quarantine,
        n_log_entries=n_log,
    )
    logger.info(
        "Loaded %d consolidated, %d quarantined, %d log entries → %s",
        n_consolidated,
        n_quarantine,
        n_log,
        db_path,
    )
    return result


def resolve_db_path(base_dir: str | Path, mode: str) -> Path:
    """Return the default SQLite file path for the given pipeline mode.

    Convention:
        seed → ``<base_dir>/data/seed.db``
        full → ``<base_dir>/data/output/full.db``

    Args:
        base_dir: Root directory of the ``excel_consolidator`` project.
        mode:     ``"seed"`` or ``"full"``.

    Returns:
        Absolute ``Path`` to the default SQLite file for that mode.

    Raises:
        ValueError: If ``mode`` is not ``"seed"`` or ``"full"``.
    """
    base = Path(base_dir)
    _mode_paths: dict[str, Path] = {
        "seed": base / "data" / "seed.db",
        "full": base / "data" / "output" / "full.db",
    }
    if mode not in _mode_paths:
        raise ValueError(f"Unknown mode '{mode}'. Expected 'seed' or 'full'.")
    return _mode_paths[mode]


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the three pipeline tables if they do not already exist.

    Uses ``CREATE TABLE IF NOT EXISTS`` so this function is safe to call in
    both seed mode (after ``_drop_tables``) and full mode (tables may already
    exist).

    Args:
        conn: Open SQLite connection.
    """
    conn.executescript(_SCHEMA_SQL)


def write_consolidated(conn: sqlite3.Connection, clean_df: pd.DataFrame) -> int:
    """Insert clean rows into the ``consolidated`` table.

    Selects only the canonical consolidated columns from ``clean_df``,
    filling any absent canonical columns with NULL.  Numeric columns
    (``quantity``, ``revenue``) are coerced to float so SQLite stores them as
    REAL rather than TEXT.

    Args:
        conn:     Open SQLite connection with the schema already initialised.
        clean_df: Validated clean DataFrame from ``validator.validate()``.

    Returns:
        Number of rows inserted.
    """
    if clean_df.empty:
        return 0
    df = _prepare_consolidated(clean_df)
    df.to_sql("consolidated", conn, if_exists="append", index=False)
    return len(df)


def write_quarantine(conn: sqlite3.Connection, quarantine_df: pd.DataFrame) -> int:
    """Insert quarantined rows into the ``quarantine`` table.

    Numeric columns are intentionally kept as strings to preserve whatever
    invalid value caused the quarantine (e.g. ``"pending"``, ``"-450"``).

    Args:
        conn:          Open SQLite connection with the schema already initialised.
        quarantine_df: Quarantined DataFrame from ``validator.validate()``,
                       with a leading ``quarantine_reason`` column.

    Returns:
        Number of rows inserted.
    """
    if quarantine_df.empty:
        return 0
    df = _prepare_quarantine(quarantine_df)
    df.to_sql("quarantine", conn, if_exists="append", index=False)
    return len(df)


def write_cleaning_log(
    conn: sqlite3.Connection,
    cleaning_log: Sequence[CleaningEntry],
) -> int:
    """Insert ``CleaningEntry`` records into the ``cleaning_log`` table.

    Converts the list of dataclass instances to a DataFrame and inserts in
    one batch.

    Args:
        conn:         Open SQLite connection with the schema already initialised.
        cleaning_log: List of ``CleaningEntry`` records produced by
                      ``consolidator.consolidate()``.

    Returns:
        Number of entries inserted.
    """
    if not cleaning_log:
        return 0
    df = _cleaning_log_to_df(cleaning_log)
    df.to_sql("cleaning_log", conn, if_exists="append", index=False)
    return len(df)


def build_summary(result: LoadResult) -> str:
    """Return a plain-English summary of a completed load operation.

    Args:
        result: ``LoadResult`` returned by ``load()``.

    Returns:
        Multi-line string, e.g.::

            Database written to: data/seed.db
              consolidated:  347 rows loaded
              quarantine:     12 rows quarantined
              cleaning_log:   89 entries logged
    """
    return (
        f"Database written to: {result.db_path}\n"
        f"  consolidated:  {result.n_consolidated} rows loaded\n"
        f"  quarantine:    {result.n_quarantine} rows quarantined\n"
        f"  cleaning_log:  {result.n_log_entries} entries logged"
    )


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL: str = """
CREATE TABLE IF NOT EXISTS consolidated (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT,
    source_row  INTEGER,
    date        TEXT,
    product     TEXT,
    region      TEXT,
    sales_rep   TEXT,
    customer    TEXT,
    quantity    REAL,
    revenue     REAL,
    loaded_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS quarantine (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    quarantine_reason TEXT NOT NULL,
    source_file       TEXT,
    source_row        INTEGER,
    date              TEXT,
    product           TEXT,
    region            TEXT,
    sales_rep         TEXT,
    customer          TEXT,
    quantity          TEXT,
    revenue           TEXT,
    quarantined_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS cleaning_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file    TEXT NOT NULL,
    transformation TEXT NOT NULL,
    original_value TEXT NOT NULL,
    new_value      TEXT NOT NULL,
    timestamp      TEXT NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _drop_tables(conn: sqlite3.Connection) -> None:
    """Drop all three pipeline tables so seed mode starts from a clean slate.

    Args:
        conn: Open SQLite connection.
    """
    for table in _ALL_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608 — table names are internal constants
    conn.commit()


def _prepare_consolidated(df: pd.DataFrame) -> pd.DataFrame:
    """Select and type-coerce the consolidated table columns from a clean DataFrame.

    Reindexes to exactly ``_CONSOLIDATED_COLS`` (absent columns become NaN →
    NULL) and coerces ``quantity`` and ``revenue`` to float so SQLite stores
    them as REAL.

    Args:
        df: Validated clean DataFrame from ``validator.validate()``.

    Returns:
        New DataFrame with exactly the consolidated table columns in declared
        order, ready for ``to_sql``.
    """
    out = df.reindex(columns=list(_CONSOLIDATED_COLS))
    for col in ("quantity", "revenue"):
        if col in out.columns:
            out = out.copy()
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _prepare_quarantine(df: pd.DataFrame) -> pd.DataFrame:
    """Select quarantine table columns from a quarantined DataFrame.

    Reindexes to exactly ``_QUARANTINE_COLS``.  Numeric columns are kept as
    strings to preserve whatever invalid value caused the quarantine.

    Args:
        df: Quarantined DataFrame from ``validator.validate()``, with a
            leading ``quarantine_reason`` column.

    Returns:
        New DataFrame with exactly the quarantine table columns in declared
        order, ready for ``to_sql``.
    """
    return df.reindex(columns=list(_QUARANTINE_COLS))


def _cleaning_log_to_df(entries: Sequence[CleaningEntry]) -> pd.DataFrame:
    """Convert a sequence of ``CleaningEntry`` records to a DataFrame.

    Produces a DataFrame whose columns match ``_LOG_COLS`` exactly, suitable
    for direct insertion via ``to_sql``.

    Args:
        entries: One or more ``CleaningEntry`` dataclass instances.

    Returns:
        DataFrame with columns ``source_file``, ``transformation``,
        ``original_value``, ``new_value``, ``timestamp``.
    """
    return pd.DataFrame(
        [
            {
                "source_file": e.source_file,
                "transformation": e.transformation,
                "original_value": e.original_value,
                "new_value": e.new_value,
                "timestamp": e.timestamp,
            }
            for e in entries
        ],
        columns=list(_LOG_COLS),
    )
