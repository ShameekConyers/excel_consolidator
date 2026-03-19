"""Row-level validation for the Excel consolidation pipeline.

Handles business-rule validation only. Structural cleaning (column renaming,
date normalization, numeric coercion, duplicate removal) is handled upstream
by consolidator.py.

Pipeline steps:

    1. Load validation rules from ``config/validation_rules.yaml``.
    2. Apply per-column checks (required, type, range, pattern) to every row.
    3. Apply a row-level minimum non-null fields check.
    4. Split the consolidated DataFrame into ``clean_df`` (all checks passed)
       and ``quarantine_df`` (at least one check failed).
    5. Attach a ``quarantine_reason`` column with semicolon-joined plain-English
       explanations to every quarantined row.
    6. Return a human-readable summary string.

Public functions:

    load_rules(config_path): Load validation rules from a YAML file.
    validate(df, rules): Split a DataFrame into clean and quarantined rows.
    summarize(clean_df, quarantine_df): Return a plain-English summary.

Note:
    Every helper is a pure function — same inputs always produce the same
    outputs and nothing is mutated in place. ``validate`` composes these
    helpers via a row-level ``df.apply`` call, then splits the result into
    two DataFrames.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_rules(config_path: str | Path) -> dict[str, Any]:
    """Load and return validation rules from a YAML config file.

    Args:
        config_path: Path to the YAML file (e.g. ``config/validation_rules.yaml``).

    Returns:
        Parsed YAML content. Top-level keys include ``columns``,
        ``min_non_null_fields``, ``flag_non_conforming_types``, and
        ``negative_revenue_allowed_files``.

    Raises:
        FileNotFoundError: If ``config_path`` does not exist.
    """
    with Path(config_path).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def validate(
    df: pd.DataFrame,
    rules: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split ``df`` into clean rows and quarantined rows using ``rules``.

    Each row is evaluated by ``_validate_row``. Rows that produce no failure
    reasons go into ``clean_df``; rows with at least one reason go into
    ``quarantine_df`` with an extra ``quarantine_reason`` column containing a
    plain-English, semicolon-separated explanation.

    Neither output DataFrame shares index state with the input — both have a
    fresh ``reset_index(drop=True)``.

    Args:
        df:    Consolidated DataFrame produced by ``consolidator.consolidate()``.
               Must contain ``source_file`` and ``source_row`` columns.
        rules: Validation rules dict returned by ``load_rules()``.

    Returns:
        A tuple of:
          - clean_df:      Rows that passed every validation rule.
          - quarantine_df: Rows that failed at least one rule, with a leading
            ``quarantine_reason`` column added.
    """
    reasons: pd.Series = df.apply(
        lambda row: "; ".join(_validate_row(row, rules)),
        axis=1,
    )

    failed_mask: pd.Series = reasons.str.len() > 0

    clean_df = df[~failed_mask].reset_index(drop=True)

    quarantine_df = df[failed_mask].copy().reset_index(drop=True)
    quarantine_df.insert(0, "quarantine_reason", reasons[failed_mask].tolist())

    return clean_df, quarantine_df


def summarize(clean_df: pd.DataFrame, quarantine_df: pd.DataFrame) -> str:
    """Return a plain-English summary of validation results.

    Includes overall totals and a per-file breakdown of clean vs quarantined
    row counts. The summary is also emitted via ``logger.info``.

    Args:
        clean_df:      DataFrame of rows that passed every validation rule.
        quarantine_df: DataFrame of rows that failed at least one rule.

    Returns:
        Multi-line summary string, e.g.::

            312 rows passed validation, 15 rows quarantined across 4 file(s).
              Q1_2024_sales.xlsx: 78 clean, 2 quarantined
              Q2_2024_sales.xlsx: 81 clean, 5 quarantined
    """
    n_clean = len(clean_df)
    n_quarantine = len(quarantine_df)

    unique_files: set[str] = set()
    if "source_file" in clean_df.columns:
        unique_files.update(clean_df["source_file"].dropna().unique())
    if "source_file" in quarantine_df.columns:
        unique_files.update(quarantine_df["source_file"].dropna().unique())
    n_files = len(unique_files)

    lines: list[str] = [
        f"{n_clean} row{'s' if n_clean != 1 else ''} passed validation, "
        f"{n_quarantine} row{'s' if n_quarantine != 1 else ''} quarantined "
        f"across {n_files} file{'s' if n_files != 1 else ''}."
    ]

    # Per-file breakdown
    if "source_file" in clean_df.columns or "source_file" in quarantine_df.columns:
        status_col = "_validation_status"
        combined = pd.concat(
            [
                clean_df.assign(**{status_col: "clean"}),
                quarantine_df.assign(**{status_col: "quarantine"}),
            ],
            ignore_index=True,
        )
        if "source_file" in combined.columns:
            for fname, group in combined.groupby("source_file"):
                n_c = (group[status_col] == "clean").sum()
                n_q = (group[status_col] == "quarantine").sum()
                lines.append(
                    f"  {_basename(str(fname))}: {n_c} clean, {n_q} quarantined"
                )

    msg = "\n".join(lines)
    logger.info(msg)
    return msg


# ---------------------------------------------------------------------------
# Row-level validation
# ---------------------------------------------------------------------------


def _validate_row(row: pd.Series, rules: dict[str, Any]) -> list[str]:
    """Return a list of plain-English failure reasons for a single row.

    An empty list means the row passed every check. The function is pure: it
    never mutates ``row`` or ``rules``.

    Checks applied in order:
      1. Row-level minimum non-null fields — failure recorded but column checks
         still continue (a row can fail both simultaneously).
      2. Per-column: required → type → range → pattern. A required or type
         failure skips subsequent checks on that column to avoid noise.

    Args:
        row:   A single row from the consolidated DataFrame, as a ``pd.Series``.
        rules: Validation rules dict from ``load_rules()``.

    Returns:
        Zero or more human-readable failure reasons.
    """
    col_rules: dict[str, dict] = rules.get("columns", {})
    flag_types: bool = rules.get("flag_non_conforming_types", True)
    allowed_neg_files: list[str] = rules.get("negative_revenue_allowed_files", [])
    min_non_null: int = rules.get("min_non_null_fields", 0)

    source_file: str = str(row.get("source_file", ""))
    source_row: str = str(row.get("source_row", "?"))

    failures: list[str] = []

    # 1. Row-level: minimum non-null fields
    non_null_msg = _check_min_non_null(
        row, col_rules, min_non_null, source_row, source_file
    )
    if non_null_msg:
        failures.append(non_null_msg)

    # 2. Per-column checks
    for col_name, col_rule in col_rules.items():
        # Column absent from row entirely
        if col_name not in row.index:
            if col_rule.get("required", False):
                failures.append(
                    f"required column '{col_name}' is missing from "
                    f"row {source_row} of {_basename(source_file)}"
                )
            continue

        value = row[col_name]

        # a. required
        req_msg = _check_required(col_name, value, col_rule, source_row, source_file)
        if req_msg:
            failures.append(req_msg)
            continue  # empty + required → skip type/range (they'd be misleading)

        # Skip further checks if value is empty and field is optional
        if _is_empty(value):
            continue

        # b. type
        if flag_types:
            type_msg = _check_type(col_name, value, col_rule, source_row, source_file)
            if type_msg:
                failures.append(type_msg)
                continue  # range/pattern are meaningless after a type failure

        # c. range
        range_msg = _check_range(
            col_name, value, col_rule, source_row, source_file, allowed_neg_files
        )
        if range_msg:
            failures.append(range_msg)

        # d. pattern
        pattern_msg = _check_pattern(col_name, value, col_rule, source_row, source_file)
        if pattern_msg:
            failures.append(pattern_msg)

    return failures


# ---------------------------------------------------------------------------
# Column-level check functions (pure, return Optional[str])
# ---------------------------------------------------------------------------


def _check_required(
    col_name: str,
    value: Any,
    col_rule: dict[str, Any],
    source_row: str,
    source_file: str,
) -> Optional[str]:
    """Return a failure reason if a required field is empty, else None.

    Args:
        col_name:    Canonical column name (e.g. ``"revenue"``).
        value:       Cell value from the DataFrame row.
        col_rule:    Rule dict for this column (from ``rules["columns"][col_name]``).
        source_row:  Row number string for the error message.
        source_file: Source filename for the error message.

    Returns:
        Plain-English failure reason, or ``None`` if the check passes.
    """
    if col_rule.get("required", False) and _is_empty(value):
        return (
            f"required field '{col_name}' is empty "
            f"in row {source_row} of {_basename(source_file)}"
        )
    return None


def _check_type(
    col_name: str,
    value: Any,
    col_rule: dict[str, Any],
    source_row: str,
    source_file: str,
) -> Optional[str]:
    """Return a failure reason if ``value`` cannot be coerced to the declared type.

    Called only when ``flag_non_conforming_types`` is True in the top-level
    rules. Type ``"text"`` always passes — any string is valid text.

    Args:
        col_name:    Canonical column name.
        value:       Non-empty cell value to inspect.
        col_rule:    Rule dict for this column.
        source_row:  Row number string for the error message.
        source_file: Source filename for the error message.

    Returns:
        Failure reason, or ``None`` if the type is acceptable.
    """
    declared_type: str = col_rule.get("type", "text")

    if declared_type == "numeric" and not _is_valid_numeric(value):
        return (
            f"'{value}' is not a valid number for '{col_name}' "
            f"in row {source_row} of {_basename(source_file)}"
        )

    if declared_type == "date" and not _is_valid_date(value):
        return (
            f"date '{value}' is not a valid date for '{col_name}' "
            f"in row {source_row} of {_basename(source_file)}"
        )

    return None


def _check_range(
    col_name: str,
    value: Any,
    col_rule: dict[str, Any],
    source_row: str,
    source_file: str,
    allowed_negative_files: list[str],
) -> Optional[str]:
    """Return a failure reason if ``value`` falls outside the declared min/max.

    For numeric columns the comparison is numeric (float). For date columns it
    is lexicographic — YYYY-MM-DD strings sort correctly when zero-padded.

    Revenue special case: if ``source_file`` is listed in
    ``allowed_negative_files``, the ``min`` bound for the ``revenue`` column is
    skipped (negative revenue is valid for returns/refund files). All other
    checks on that row still apply.

    Assumes the value has already passed the type check.

    Args:
        col_name:               Canonical column name.
        value:                  Non-empty, type-valid cell value.
        col_rule:               Rule dict for this column.
        source_row:             Row number string for the error message.
        source_file:            Source filename for the error message.
        allowed_negative_files: Basenames of files where negative revenue is permitted.

    Returns:
        Failure reason, or ``None`` if the range check passes.
    """
    declared_type: str = col_rule.get("type", "text")
    col_min = col_rule.get("min")
    col_max = col_rule.get("max")

    if declared_type == "numeric":
        try:
            float_val = float(str(value).strip())
        except (ValueError, TypeError):
            return None  # type check already caught this; skip silently

        # Revenue exemption for returns/refund files
        skip_min = (
            col_name == "revenue"
            and _basename(source_file) in allowed_negative_files
        )
        effective_min = None if skip_min else col_min

        if effective_min is not None and float_val < float(effective_min):
            adjective = "negative" if float_val < 0 else "below minimum"
            return (
                f"{col_name} is {adjective} ({float_val:g}) "
                f"in row {source_row} of {_basename(source_file)}"
            )

        if col_max is not None and float_val > float(col_max):
            return (
                f"{col_name} exceeds maximum ({float_val:g} > {col_max}) "
                f"in row {source_row} of {_basename(source_file)}"
            )

    elif declared_type == "date":
        # Lexicographic comparison is valid for zero-padded YYYY-MM-DD strings
        date_str = str(value).strip()
        min_date = str(col_min) if col_min is not None else None
        max_date = str(col_max) if col_max is not None else None

        if min_date and date_str < min_date:
            return (
                f"date '{date_str}' is before minimum allowed date "
                f"'{min_date}' in row {source_row} of {_basename(source_file)}"
            )
        if max_date and date_str > max_date:
            return (
                f"date '{date_str}' is after maximum allowed date "
                f"'{max_date}' in row {source_row} of {_basename(source_file)}"
            )

    return None


def _check_pattern(
    col_name: str,
    value: Any,
    col_rule: dict[str, Any],
    source_row: str,
    source_file: str,
) -> Optional[str]:
    """Return a failure reason if ``value`` does not match the declared regex.

    Uses ``re.fullmatch`` — the pattern must match the entire value, not just
    a substring. If the column has no ``pattern`` key, returns None.

    Args:
        col_name:    Canonical column name.
        value:       Non-empty cell value to test.
        col_rule:    Rule dict for this column.
        source_row:  Row number string for the error message.
        source_file: Source filename for the error message.

    Returns:
        Failure reason, or ``None`` if the pattern matches (or is absent).
    """
    pattern: Optional[str] = col_rule.get("pattern")
    if not pattern:
        return None

    if not _is_valid_pattern(str(value), pattern):
        return (
            f"'{value}' does not match required pattern for '{col_name}' "
            f"in row {source_row} of {_basename(source_file)}"
        )
    return None


def _check_min_non_null(
    row: pd.Series,
    col_rules: dict[str, dict],
    min_non_null: int,
    source_row: str,
    source_file: str,
) -> Optional[str]:
    """Return a failure reason if ``row`` has fewer than ``min_non_null`` populated fields.

    Only the canonical data columns (those defined in ``col_rules``) are
    counted. Metadata columns such as ``source_file`` and ``source_row`` are
    excluded from the count.

    Args:
        row:          A single row from the consolidated DataFrame.
        col_rules:    Mapping of canonical column names to their rule dicts.
        min_non_null: Minimum number of non-empty data-column values required.
        source_row:   Row number string for the error message.
        source_file:  Source filename for the error message.

    Returns:
        Failure reason, or ``None`` if the row has enough populated fields.
    """
    if min_non_null <= 0:
        return None

    data_cols: list[str] = list(col_rules.keys())
    non_null_count: int = sum(
        1 for col in data_cols if col in row.index and not _is_empty(row[col])
    )

    if non_null_count < min_non_null:
        return (
            f"row has only {non_null_count} non-empty field(s) "
            f"(minimum {min_non_null} required) "
            f"in row {source_row} of {_basename(source_file)}"
        )
    return None


# ---------------------------------------------------------------------------
# Primitive predicates (pure, no side effects)
# ---------------------------------------------------------------------------


def _is_empty(value: Any) -> bool:
    """Return True if ``value`` should be treated as missing or empty.

    Covers: ``None``, float NaN, empty string, whitespace-only string, and the
    string literal ``"nan"`` / ``"NaN"`` (stringified NaN from pandas).

    Args:
        value: Any cell value from a pandas DataFrame.

    Returns:
        True if the value is considered empty, False otherwise.
    """
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        # pd.isna raises on unhashable types (e.g. lists) — treat as non-empty
        pass
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.lower() == "nan"
    return False


def _is_valid_numeric(value: Any) -> bool:
    """Return True if ``value`` can be coerced to a finite float.

    The consolidator strips currency symbols and commas upstream, so this
    function only needs a plain ``float()`` conversion attempt.

    Args:
        value: Cell value to test.

    Returns:
        True if the value is float-coercible, False otherwise.
    """
    if _is_empty(value):
        return False
    try:
        float(str(value).strip())
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_date(value: Any) -> bool:
    """Return True if ``value`` is a real YYYY-MM-DD calendar date.

    Uses ``datetime.strptime`` rather than regex or ``pd.to_datetime`` so that
    structurally plausible but impossible dates — e.g. ``"2024-13-01"``
    (month 13) or ``"2024-02-30"`` (Feb 30) — are correctly rejected.

    Args:
        value: Cell value to test. Expected to be a YYYY-MM-DD string after
               consolidation; other formats return False.

    Returns:
        True if the value is a valid calendar date, False otherwise.
    """
    if _is_empty(value):
        return False
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_pattern(value: str, pattern: str) -> bool:
    """Return True if ``value`` fully matches the regular expression ``pattern``.

    Uses ``re.fullmatch`` — the pattern must cover the entire string. If
    ``pattern`` is malformed, returns True (silently passes) rather than
    crashing every row due to a configuration error.

    Args:
        value:   String to test.
        pattern: Regular expression string.

    Returns:
        True if the value matches the pattern, False otherwise.
    """
    try:
        return re.fullmatch(pattern, value) is not None
    except re.error:
        return True  # malformed YAML pattern — don't quarantine due to config bug


def _basename(file_path: str) -> str:
    """Return the filename component of ``file_path`` without its directory.

    Used to produce short, readable names (e.g. ``"Q3_sales.xlsx"``) in
    quarantine reason messages rather than full absolute paths.

    Args:
        file_path: Absolute or relative path string.

    Returns:
        Just the filename (e.g. ``"Q3_sales.xlsx"``), or the original string
        if ``file_path`` is empty.
    """
    if not file_path:
        return file_path
    return Path(file_path).name
