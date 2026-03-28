"""Streamlit utility dashboard for the Excel-to-Database Consolidator pipeline.

Displays pipeline results from the SQLite database produced by
``scripts/run_pipeline.py``: KPI metrics, per-file data quality, a
filterable quarantine viewer, and a clean data preview.  An export button
in the sidebar generates a downloadable ``.xlsx`` workbook on demand.

Design — functional style:
    Data flows through six layers of pure functions with no shared mutable
    state.  The only side effects are Streamlit widget renders and the
    temporary ``.xlsx`` file written by the export layer.

        DB_PATH ──► load_data (cached) ──► filter_* helpers ──► render_* functions
                 ──► load_cleaning_summary (cached)
                 ──► load_quarantine_summary (cached)

Usage:
    .venv/bin/streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# sys.path — make src/ importable from any working directory
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import altair as alt
import pandas as pd
import streamlit as st

import export as _export
import report as _report
from report import CleaningSummary, QuarantineSummary

# ---------------------------------------------------------------------------
# Layer 1 — Constants
# ---------------------------------------------------------------------------

DB_PATH: Path = Path(__file__).parent.parent / "data" / "seed.db"
OUTPUT_DIR: Path = Path(__file__).parent.parent / "data" / "output"

# Ordered mapping from human-readable category labels to the keyword fragments
# that appear in plain-English quarantine_reason strings from validator.py.
# Mirrors _classify_reason in src/report.py so filter categories match the
# QuarantineSummary breakdown exactly.
_REASON_KEYWORDS: list[tuple[str, list[str]]] = [
    ("missing required field", ["required field"]),
    ("type mismatch", ["is not a valid number"]),
    ("invalid date", ["is not a valid date"]),
    ("negative value", ["is negative"]),
    ("out of range", ["is below minimum", "exceeds maximum", "is before minimum", "is after maximum"]),
    ("pattern mismatch", ["does not match pattern"]),
    ("sparse row", ["too few non-null"]),
]


# ---------------------------------------------------------------------------
# Layer 2 — Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data
def load_data(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all three pipeline tables from SQLite in a single cached call.

    Args:
        db_path: Absolute path to the SQLite database.

    Returns:
        Three-tuple of (clean_df, quarantine_df, log_df). Each DataFrame
        has internal database columns (id, loaded_at, quarantined_at)
        already removed by the src/ readers.

    Raises:
        FileNotFoundError: If db_path does not exist.
    """
    clean_df = _report.read_consolidated(db_path)
    quarantine_df = _report.read_quarantine(db_path)
    log_df = _report.read_cleaning_log(db_path)
    return clean_df, quarantine_df, log_df


@st.cache_data
def load_cleaning_summary(db_path: Path) -> CleaningSummary:
    """Compute and cache the CleaningSummary from the pipeline tables.

    Args:
        db_path: Absolute path to the SQLite database.

    Returns:
        CleaningSummary with file counts, row counts, and transformation
        metrics derived from the cleaning_log table.
    """
    clean_df, quarantine_df, log_df = load_data(db_path)
    return _report.generate_cleaning_summary(clean_df, quarantine_df, log_df)


@st.cache_data
def load_quarantine_summary(db_path: Path) -> QuarantineSummary:
    """Compute and cache the QuarantineSummary from the quarantine table.

    Args:
        db_path: Absolute path to the SQLite database.

    Returns:
        QuarantineSummary with total count and breakdowns by reason type
        and source file.
    """
    _, quarantine_df, _ = load_data(db_path)
    return _report.generate_quarantine_summary(quarantine_df)


# ---------------------------------------------------------------------------
# Layer 3 — Pure filter / transform helpers
# ---------------------------------------------------------------------------


def _categorize_reason(reason: str) -> str:
    """Map a quarantine_reason string to its human-readable category label.

    Mirrors the _classify_reason logic in src/report.py so that filter
    drop-down categories match the QuarantineSummary breakdown exactly.

    Args:
        reason: Plain-English quarantine_reason string from validator.py,
                e.g. "revenue is negative (-450) in row 23 of Q1.xlsx".

    Returns:
        Category label matching a key in _REASON_KEYWORDS, or "other".
    """
    lc = reason.lower()
    for category, keywords in _REASON_KEYWORDS:
        if any(kw in lc for kw in keywords):
            return category
    return "other"


def filter_quarantine(
    quarantine_df: pd.DataFrame,
    source_file: Optional[str],
    reason_type: Optional[str],
) -> pd.DataFrame:
    """Return quarantined rows matching the given filter values.

    Args:
        quarantine_df: Full quarantine DataFrame from load_data.
        source_file:   Filename to filter on, or None to keep all files.
        reason_type:   Reason category label to filter on (matched via
                       _categorize_reason), or None to keep all reasons.

    Returns:
        Filtered DataFrame. The original DataFrame is not mutated.
    """
    df = quarantine_df
    if source_file:
        df = df[df["source_file"] == source_file]
    if reason_type:
        mask = df["quarantine_reason"].fillna("").apply(_categorize_reason) == reason_type
        df = df[mask]
    return df


def filter_consolidated(
    clean_df: pd.DataFrame,
    source_file: Optional[str],
    region: Optional[str],
    date_range: Optional[tuple[str, str]],
) -> pd.DataFrame:
    """Return clean rows matching the given filter values.

    Args:
        clean_df:    Full consolidated DataFrame from load_data.
        source_file: Filename to filter on, or None.
        region:      Region value to filter on, or None.
        date_range:  Two-element tuple of (start_date, end_date) as
                     YYYY-MM-DD strings, or None for no date filter.

    Returns:
        Filtered DataFrame. The original DataFrame is not mutated.
    """
    df = clean_df
    if source_file:
        df = df[df["source_file"] == source_file]
    if region:
        df = df[df["region"] == region]
    if date_range:
        start, end = date_range
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    return df


def build_per_file_quality_table(
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a per-file data quality summary table for the Overview page.

    Computes rows loaded, rows quarantined, and quarantine rate for every
    source file that appears in either table.

    Args:
        clean_df:      Consolidated rows DataFrame.
        quarantine_df: Quarantine rows DataFrame.

    Returns:
        DataFrame with columns source_file, rows_loaded, rows_quarantined,
        quarantine_rate_pct. Sorted ascending by source_file.
    """
    clean_counts: pd.Series = (
        clean_df.groupby("source_file").size().rename("rows_loaded")
        if not clean_df.empty
        else pd.Series(dtype=int, name="rows_loaded")
    )
    quarantine_counts: pd.Series = (
        quarantine_df.groupby("source_file").size().rename("rows_quarantined")
        if not quarantine_df.empty
        else pd.Series(dtype=int, name="rows_quarantined")
    )
    quality = (
        pd.concat([clean_counts, quarantine_counts], axis=1)
        .fillna(0)
        .astype(int)
    )
    quality.index.name = "source_file"
    quality = quality.reset_index().sort_values("source_file")
    total = quality["rows_loaded"] + quality["rows_quarantined"]
    quality["quarantine_rate_pct"] = (
        quality["rows_quarantined"] / total.replace(0, 1) * 100
    ).round(1)
    return quality.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Layer 4 — Pure render functions
# ---------------------------------------------------------------------------


def render_kpi_cards(
    cleaning: CleaningSummary,
    quarantine: QuarantineSummary,
) -> None:
    """Render the five top-level KPI metric cards.

    Displays files processed, total rows ingested, rows passed, rows
    quarantined, and columns standardised via five st.metric widgets in a
    single st.columns(5) row. The quarantine count uses delta_color="inverse"
    so it turns red when non-zero, signalling rows that need review.

    Args:
        cleaning:   CleaningSummary from load_cleaning_summary.
        quarantine: QuarantineSummary from load_quarantine_summary.
    """
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Files Processed", cleaning.n_files)
    c2.metric("Total Rows Ingested", cleaning.n_rows_before)
    c3.metric("Rows Passed", cleaning.n_rows_after)
    c4.metric(
        "Rows Quarantined",
        quarantine.n_quarantined,
        delta=f"-{quarantine.n_quarantined}" if quarantine.n_quarantined else None,
        delta_color="inverse",
    )
    c5.metric("Columns Standardised", cleaning.n_columns_standardized)


def render_per_file_quality_table(quality_df: pd.DataFrame) -> None:
    """Render the per-file data quality breakdown table.

    Displays the DataFrame produced by build_per_file_quality_table with
    percentage formatting on the quarantine_rate_pct column.

    Args:
        quality_df: DataFrame from build_per_file_quality_table.
    """
    st.dataframe(
        quality_df,
        column_config={
            "source_file": st.column_config.TextColumn("File", width="large"),
            "rows_loaded": st.column_config.NumberColumn("Rows Loaded"),
            "rows_quarantined": st.column_config.NumberColumn("Rows Quarantined"),
            "quarantine_rate_pct": st.column_config.NumberColumn(
                "Quarantine Rate (%)", format="%.1f"
            ),
        },
        use_container_width=True,
        hide_index=True,
    )


def render_quarantine_table(quarantine_df: pd.DataFrame) -> None:
    """Render the quarantine viewer table.

    Displays source_file, source_row, quarantine_reason, date, product,
    region, quantity, and revenue columns. The quarantine_reason column is
    set to width="large" so reasons are readable without horizontal scrolling.

    Args:
        quarantine_df: Filtered quarantine DataFrame from filter_quarantine.
    """
    display_cols = [
        c for c in
        ["source_file", "source_row", "quarantine_reason", "date",
         "product", "region", "quantity", "revenue"]
        if c in quarantine_df.columns
    ]
    st.dataframe(
        quarantine_df[display_cols],
        column_config={
            "quarantine_reason": st.column_config.TextColumn(
                "Quarantine Reason", width="large"
            ),
            "source_file": st.column_config.TextColumn("File", width="medium"),
        },
        use_container_width=True,
        hide_index=True,
    )


def render_clean_data_table(clean_df: pd.DataFrame) -> None:
    """Render the clean data preview table.

    Displays all columns except source_row to keep the table concise.

    Args:
        clean_df: Filtered consolidated DataFrame from filter_consolidated.
    """
    display_cols = [c for c in clean_df.columns if c != "source_row"]
    st.dataframe(
        clean_df[display_cols],
        use_container_width=True,
        hide_index=True,
    )


def render_file_quality_chart(quality_df: pd.DataFrame) -> None:
    """Render a stacked horizontal bar chart of clean vs quarantined rows per file.

    Args:
        quality_df: DataFrame from build_per_file_quality_table with columns
                    source_file, rows_loaded, rows_quarantined.
    """
    if quality_df.empty:
        return
    melted = quality_df.melt(
        id_vars="source_file",
        value_vars=["rows_loaded", "rows_quarantined"],
        var_name="status",
        value_name="rows",
    )
    melted["status"] = melted["status"].map(
        {"rows_loaded": "Passed", "rows_quarantined": "Quarantined"}
    )
    chart = (
        alt.Chart(melted)
        .mark_bar()
        .encode(
            y=alt.Y("source_file:N", sort="-x", title=None),
            x=alt.X("rows:Q", title="Rows"),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(
                    domain=["Passed", "Quarantined"],
                    range=["#4CAF50", "#FF5252"],
                ),
                title="Status",
            ),
            tooltip=["source_file", "status", "rows"],
        )
        .properties(height=max(len(quality_df) * 40, 200))
    )
    st.altair_chart(chart, use_container_width=True)


def render_quarantine_reasons_chart(quarantine_summary: QuarantineSummary) -> None:
    """Render a horizontal bar chart of quarantine counts by reason type.

    Args:
        quarantine_summary: QuarantineSummary with by_reason_type dict.
    """
    if not quarantine_summary.by_reason_type:
        return
    df = pd.DataFrame(
        list(quarantine_summary.by_reason_type.items()),
        columns=["reason", "count"],
    ).sort_values("count", ascending=False)
    chart = (
        alt.Chart(df)
        .mark_bar(color="#FF8A65")
        .encode(
            y=alt.Y("reason:N", sort="-x", title=None),
            x=alt.X("count:Q", title="Rows"),
            tooltip=["reason", "count"],
        )
        .properties(height=max(len(df) * 40, 150))
    )
    st.altair_chart(chart, use_container_width=True)


def render_monthly_revenue_chart(clean_df: pd.DataFrame) -> None:
    """Render a line chart of monthly revenue over time.

    Args:
        clean_df: Filtered consolidated DataFrame with date and revenue columns.
    """
    if clean_df.empty or "date" not in clean_df.columns:
        return
    df = clean_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "revenue"])
    if df.empty:
        return
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby("month")["revenue"].sum().reset_index()
    chart = (
        alt.Chart(monthly)
        .mark_line(point=True, color="#1E88E5")
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("revenue:Q", title="Revenue ($)"),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%b %Y"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.0f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)


def render_revenue_by_region_chart(clean_df: pd.DataFrame) -> None:
    """Render a bar chart of total revenue by region.

    Args:
        clean_df: Filtered consolidated DataFrame with region and revenue columns.
    """
    if clean_df.empty or "region" not in clean_df.columns:
        return
    by_region = (
        clean_df.groupby("region")["revenue"]
        .sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    chart = (
        alt.Chart(by_region)
        .mark_bar(color="#42A5F5")
        .encode(
            x=alt.X("region:N", sort="-y", title="Region"),
            y=alt.Y("revenue:Q", title="Revenue ($)"),
            tooltip=[
                alt.Tooltip("region:N"),
                alt.Tooltip("revenue:Q", format="$,.0f"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)


def render_revenue_by_product_chart(clean_df: pd.DataFrame) -> None:
    """Render a horizontal bar chart of total revenue by product.

    Args:
        clean_df: Filtered consolidated DataFrame with product and revenue columns.
    """
    if clean_df.empty or "product" not in clean_df.columns:
        return
    by_product = (
        clean_df.groupby("product")["revenue"]
        .sum()
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    chart = (
        alt.Chart(by_product)
        .mark_bar(color="#7E57C2")
        .encode(
            y=alt.Y("product:N", sort="-x", title=None),
            x=alt.X("revenue:Q", title="Revenue ($)"),
            tooltip=[
                alt.Tooltip("product:N"),
                alt.Tooltip("revenue:Q", format="$,.0f"),
            ],
        )
        .properties(height=max(len(by_product) * 45, 200))
    )
    st.altair_chart(chart, use_container_width=True)


def render_export_button(db_path: Path, output_dir: Path) -> None:
    """Render the export button in the sidebar and serve the generated .xlsx.

    Calls export() from src/export.py to write a workbook to output_dir,
    reads the bytes back, and serves them via st.sidebar.download_button.
    The export runs once per session (DataFrames are cached) and completes
    in under a second for typical seed-database sizes.

    Args:
        db_path:    Path to the SQLite database.
        output_dir: Directory where the temporary .xlsx is written.
                    This directory is gitignored (data/output/).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "consolidated_export.xlsx"
    _export.export(db_path, output_path)
    xlsx_bytes = output_path.read_bytes()
    st.sidebar.download_button(
        label="Download .xlsx",
        data=xlsx_bytes,
        file_name="consolidated_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------------------------------------------------------------------
# Layer 5 — Page renderers
# ---------------------------------------------------------------------------


def render_overview_page(
    cleaning: CleaningSummary,
    quarantine: QuarantineSummary,
    clean_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
) -> None:
    """Render the Overview page with KPI cards, charts, and quality table.

    Args:
        cleaning:      CleaningSummary dataclass from load_cleaning_summary.
        quarantine:    QuarantineSummary dataclass from load_quarantine_summary.
        clean_df:      Full consolidated DataFrame.
        quarantine_df: Full quarantine DataFrame.
    """
    st.subheader("Pipeline Summary")
    render_kpi_cards(cleaning, quarantine)
    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Rows by File")
        quality_df = build_per_file_quality_table(clean_df, quarantine_df)
        render_file_quality_chart(quality_df)
    with col_right:
        st.subheader("Quarantine Reasons")
        render_quarantine_reasons_chart(quarantine)

    st.divider()
    st.subheader("Data Quality by File")
    render_per_file_quality_table(quality_df)


def render_quarantine_by_file_chart(quarantine_df: pd.DataFrame) -> None:
    """Render a bar chart of quarantined row counts by source file.

    Args:
        quarantine_df: Full quarantine DataFrame from load_data.
    """
    if quarantine_df.empty:
        return
    by_file = (
        quarantine_df.groupby("source_file")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    chart = (
        alt.Chart(by_file)
        .mark_bar(color="#FF5252")
        .encode(
            y=alt.Y("source_file:N", sort="-x", title=None),
            x=alt.X("count:Q", title="Quarantined Rows"),
            tooltip=["source_file", "count"],
        )
        .properties(height=max(len(by_file) * 40, 150))
    )
    st.altair_chart(chart, use_container_width=True)


def render_quarantine_page(quarantine_df: pd.DataFrame) -> None:
    """Render the Quarantine Viewer page with charts, filters, and data table.

    Builds source_file and reason_type selectboxes from the data, calls
    filter_quarantine with the selected values, and renders the result with
    a row count caption.

    Args:
        quarantine_df: Full quarantine DataFrame from load_data.
    """
    n_total = len(quarantine_df)
    st.subheader("Quarantined Rows")
    st.caption(f"{n_total} rows held for review")

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("**By Reason**")
        if not quarantine_df.empty:
            reason_counts = (
                quarantine_df["quarantine_reason"]
                .fillna("")
                .apply(_categorize_reason)
                .value_counts()
                .reset_index()
            )
            reason_counts.columns = ["reason", "count"]
            chart = (
                alt.Chart(reason_counts)
                .mark_bar(color="#FF8A65")
                .encode(
                    y=alt.Y("reason:N", sort="-x", title=None),
                    x=alt.X("count:Q", title="Rows"),
                    tooltip=["reason", "count"],
                )
                .properties(height=max(len(reason_counts) * 40, 150))
            )
            st.altair_chart(chart, use_container_width=True)
    with col_right:
        st.markdown("**By File**")
        render_quarantine_by_file_chart(quarantine_df)

    st.divider()

    all_files: list[str] = (
        sorted(quarantine_df["source_file"].dropna().unique())
        if not quarantine_df.empty else []
    )
    all_reasons: list[str] = sorted(set(
        _categorize_reason(r)
        for r in quarantine_df["quarantine_reason"].dropna()
    )) if not quarantine_df.empty else []

    col1, col2 = st.columns(2)
    with col1:
        file_choice: str = st.selectbox(
            "Filter by file", ["All"] + all_files, key="q_file"
        )
    with col2:
        reason_choice: str = st.selectbox(
            "Filter by reason", ["All"] + all_reasons, key="q_reason"
        )

    filtered = filter_quarantine(
        quarantine_df,
        source_file=file_choice if file_choice != "All" else None,
        reason_type=reason_choice if reason_choice != "All" else None,
    )
    render_quarantine_table(filtered)
    st.caption(f"Showing {len(filtered)} of {n_total} rows")


def render_clean_data_page(clean_df: pd.DataFrame) -> None:
    """Render the Clean Data page with filter controls and data preview.

    Builds source_file, region, and date range inputs from the data, calls
    filter_consolidated with the selected values, and renders the result.

    Args:
        clean_df: Full consolidated DataFrame from load_data.
    """
    n_total = len(clean_df)
    st.subheader("Sales")
    st.caption(f"{n_total} rows passed validation")

    all_files: list[str] = (
        sorted(clean_df["source_file"].dropna().unique())
        if not clean_df.empty else []
    )
    all_regions: list[str] = (
        sorted(clean_df["region"].dropna().unique())
        if not clean_df.empty else []
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        file_choice: str = st.selectbox(
            "Filter by file", ["All"] + all_files, key="c_file"
        )
    with col2:
        region_choice: str = st.selectbox(
            "Filter by region", ["All"] + all_regions, key="c_region"
        )
    with col3:
        date_range_raw = None
        if "date" in clean_df.columns and not clean_df.empty:
            dates = pd.to_datetime(clean_df["date"], errors="coerce").dropna()
            if not dates.empty:
                date_range_raw = st.date_input(
                    "Date range",
                    value=(dates.min().date(), dates.max().date()),
                    min_value=dates.min().date(),
                    max_value=dates.max().date(),
                    key="c_date",
                )

    date_range_strs: Optional[tuple[str, str]] = None
    if (
        date_range_raw is not None
        and isinstance(date_range_raw, (list, tuple))
        and len(date_range_raw) == 2
    ):
        date_range_strs = (str(date_range_raw[0]), str(date_range_raw[1]))

    filtered = filter_consolidated(
        clean_df,
        source_file=file_choice if file_choice != "All" else None,
        region=region_choice if region_choice != "All" else None,
        date_range=date_range_strs,
    )

    st.subheader("Revenue Trend")
    render_monthly_revenue_chart(filtered)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Revenue by Region")
        render_revenue_by_region_chart(filtered)
    with col_right:
        st.subheader("Revenue by Product")
        render_revenue_by_product_chart(filtered)

    st.divider()
    st.subheader("Data Table")
    render_clean_data_table(filtered)
    st.caption(f"Showing {len(filtered)} of {n_total} rows")


# ---------------------------------------------------------------------------
# Layer 6 — Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Configure the Streamlit app and route between the three pages.

    Loads data once via cached loaders, renders the export button in the
    sidebar, then uses st.tabs to route between Overview, Quarantine, and
    Clean Data pages.
    """
    st.set_page_config(
        page_title="Excel Consolidator",
        page_icon=":bar_chart:",
        layout="wide",
    )

    st.title("Excel-to-Database Consolidator")
    st.caption("Pipeline results from data/seed.db")

    if not DB_PATH.exists():
        st.error(
            f"Database not found: {DB_PATH}. "
            "Run `scripts/run_pipeline.py --mode seed` first."
        )
        return

    clean_df, quarantine_df, _ = load_data(DB_PATH)
    cleaning = load_cleaning_summary(DB_PATH)
    quarantine_summary = load_quarantine_summary(DB_PATH)

    with st.sidebar:
        st.header("Export")
        render_export_button(DB_PATH, OUTPUT_DIR)
        st.divider()
        st.caption("Reads from data/seed.db. Re-run the pipeline to refresh.")

    tab1, tab2, tab3 = st.tabs(["Sales Dashboard", "Pipeline Overview", "Quarantine"])

    with tab1:
        render_clean_data_page(clean_df)

    with tab2:
        render_overview_page(cleaning, quarantine_summary, clean_df, quarantine_df)

    with tab3:
        render_quarantine_page(quarantine_df)


if __name__ == "__main__":
    main()
