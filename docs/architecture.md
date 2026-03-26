# Architecture: File-by-File Reference

Technical reference for every file in the project — what it does, what it exposes,
and how it fits into the pipeline.

---

## Pipeline Flow

```
data/sample_files/  ──►  consolidator.py  ──►  validator.py
  (or Google Drive)                                  │
        ▲                                    ┌───────┴────────┐
        │                                    ▼                ▼
  drive_connector.py                     clean_df       quarantine_df
                                              │                │
                                              └──────┬─────────┘
                                                     ▼
                                               db_loader.py
                                              ┌──────┴──────┐
                                              ▼             ▼
                                        consolidated    quarantine
                                           table          table
                                              │
                                        cleaning_log
                                           table
                                              │
                               ┌─────────────┼─────────────┐
                               ▼             ▼             ▼
                          export.py      report.py     dashboard/app.py
                        (Excel out)   (.md report)    (Streamlit UI)
```

---

## `src/` — Source Modules

### `src/drive_connector.py` ✅ built

Optional Google Drive integration layer. Wraps the Drive v3 API so the rest of the
pipeline never touches Drive directly — it just receives a local folder path.

**Public functions:**

| Function | Args | Returns | Notes |
|----------|------|---------|-------|
| `authenticate()` | — | Drive service resource | Tries `credentials.json` first, falls back to `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` env vars. Writes `token.json` after first consent. |
| `list_files(service, folder_id)` | service, folder ID str | `list[dict]` with `id`, `name`, `mimeType` | Paginated. Filters to `.xlsx`/`.xls`/`.csv` only. |
| `download_file(service, file_id, dest_path)` | service, file ID, local path | `Path` | Streams to disk with `MediaIoBaseDownload`. |
| `upload_file(service, local_path, folder_id)` | service, local path, folder ID | Drive file ID str | Resumable upload via `MediaFileUpload`. |
| `read_file(service, file_id)` | service, file ID | `bytes` | In-memory only — no local write. |
| `create_folder(service, folder_name, parent_id)` | service, name str, optional parent ID str | Drive folder ID str | `parent_id=None` creates in Drive root. |

**Auth credential lookup order:**
1. `credentials.json` in project root (downloaded from GCP Console)
2. `GOOGLE_CLIENT_ID` + `GOOGLE_CLIENT_SECRET` in `.env`
3. `RuntimeError` with setup instructions if neither is found

**CLI:** `.venv/bin/python src/drive_connector.py --auth` — quick auth smoke-test.

---

### `src/consolidator.py` ✅ built

Core ingestion and standardization module. Accepts a folder path (local or a temp
folder populated by `drive_connector.py`) and produces a single merged DataFrame.

**Responsibilities:**
- Discover all `.xlsx`, `.xls`, `.csv` files in the input folder (skips `~$` lock files)
- Read each file with no header parsing (`dtype=str`) to preserve raw values
- Auto-detect the true header row — tolerates title/spacer rows above it (e.g. `west_region_2024.xlsx` has two non-data rows before its header)
- Rename columns to canonical names via `COLUMN_MAP` (30+ variant → 7 canonical)
- Inject a `region = "West"` column for files that omit it (filename contains "west")
- Parse and normalize all date columns to ISO 8601 (`YYYY-MM-DD`); leave unparseable values unchanged for the validator
- Strip currency symbols and commas from `quantity`/`revenue`; leave non-numeric values unchanged for the validator
- Attach `source_file` and `source_row` (1-based Excel row number) to every row **before** any rows are dropped
- Drop fully-empty rows; detect and remove exact cross-file duplicates
- Append every transformation to a shared `list[CleaningEntry]`

> **TODO — multi-sheet support:** `_read_raw()` currently calls `pd.read_excel(header=None)` which reads only the first sheet. Some workbooks store data across multiple sheets (e.g. one sheet per region or month). Scoping needed: (1) detect whether a workbook has multiple data sheets vs. one data sheet + metadata sheets; (2) decide whether to concatenate all data sheets or let the user configure a target sheet name/index per file in `config/validation_rules.yaml`; (3) update `detect_header_row` and `tag_source` to include the sheet name in `source_file` (e.g. `"Q1_sales.xlsx [East]"`). None of the current sample files require this — all are single-sheet — so this is a v2 addition.

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `consolidate(folder_path)` | `str \| Path` | `tuple[DataFrame, list[CleaningEntry]]` |
| `load_file(file_path, log)` | path, log list | `DataFrame` |
| `standardize_columns(df, source_file, log)` | df, filename, log | `DataFrame` |
| `normalize_dates(df, source_file, log)` | df, filename, log | `DataFrame` |
| `clean_numeric_columns(df, source_file, log)` | df, filename, log | `DataFrame` |
| `remove_duplicates(df, log)` | df, log | `DataFrame` |
| `handle_missing_values(df, source_file, log, strategy)` | df, filename, log, `"drop_empty"` or `"flag"` | `DataFrame` |
| `tag_source(df, file_path, header_row_idx)` | df, Path, int | `DataFrame` |
| `log_action(log, source_file, transformation, original_value, new_value)` | — | `None` |
| `detect_header_row(raw_df)` | df with integer columns | `int` |

**Class:** `CleaningEntry` — dataclass with fields `source_file`, `transformation`, `original_value`, `new_value`, `timestamp` (UTC ISO-8601).

**Constants:** `CANONICAL_COLUMNS` (frozenset), `COLUMN_MAP` (dict).

**Consumed by:** `validator.py` (imports `CleaningEntry`, `log_action`, `CANONICAL_COLUMNS`), `db_loader.py` (imports `consolidate`, `CleaningEntry`).

---

### `src/validator.py` ✅ built

Splits the consolidated DataFrame into clean rows and quarantined rows by running
each row through the rules defined in `config/validation_rules.yaml`.

**Responsibilities:**
- Load rules from YAML (types, min/max, required fields, file-level exceptions)
- For each row, run all applicable checks
- Any row failing at least one check goes to `quarantine_df` with a
  `quarantine_reason` string describing every failure in plain English
- Rows passing all checks go to `clean_df`
- Log summary counts

**Checks performed:**
| Check | Example quarantine reason |
|-------|--------------------------|
| Type mismatch (text in numeric) | `"quantity 'TBD' is not numeric in row 9 of Q3_2024_sales.xlsx"` |
| Date out of allowed range | `"date '2099-01-01' is out of range in row 14 of Q4_2024_sales.xlsx"` |
| Impossible date | `"date '2024-13-01' is not a valid date in row 3 of Q3_2024_sales.xlsx"` |
| Negative revenue (sales files) | `"revenue is negative (-450.00) in row 5 of Q1_2024_sales.xlsx"` |
| Missing required field | `"required field 'customer' is empty in row 12 of Q1_2024_sales.xlsx"` |
| Entirely empty row | `"row is empty in row 22 of Q1_2024_sales.xlsx"` |
| Comment/note row | `"date 'Mike please update...' is not a valid date in row 31 of Q2_2024_sales.xlsx"` |

**File-level exception:** `returns_flagged.csv` is exempt from `revenue min: 0`
(configured under `negative_revenue_allowed_files` in the YAML).

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `load_rules(config_path)` | `str \| Path` | `dict` of parsed YAML rules |
| `validate(df, rules)` | DataFrame, rules dict | `tuple[clean_df, quarantine_df]` |
| `summarize(clean_df, quarantine_df)` | two DataFrames | plain-English summary string |

---

### `src/db_loader.py` ✅ built

Loads the two DataFrames from `validator.py` and the cleaning log into SQLite.

**Tables created:**

| Table | Contents |
|-------|----------|
| `consolidated` | All rows that passed validation |
| `quarantine` | All rows that failed, with original data intact + `quarantine_reason` |
| `cleaning_log` | Every transformation applied (column renames, type casts, duplicates removed) |

**Modes:**
- `--seed` (default) — reads from `data/sample_files/`, writes to `data/seed.db`
- `--full` — reads from a live folder or Drive, writes to a separate DB

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `load(clean_df, quarantine_df, cleaning_log, db_path, mode)` | DataFrames, log, path, `"seed"` or `"full"` | `LoadResult` |
| `resolve_db_path(base_dir, mode)` | project root path, mode str | `Path` to default DB file |
| `init_schema(conn)` | SQLite connection | — |
| `write_consolidated(conn, clean_df)` | connection, DataFrame | rows inserted (int) |
| `write_quarantine(conn, quarantine_df)` | connection, DataFrame | rows inserted (int) |
| `write_cleaning_log(conn, cleaning_log)` | connection, list of `CleaningEntry` | entries inserted (int) |
| `build_summary(result)` | `LoadResult` | plain-English summary string |

**Class:** `LoadResult` — dataclass with `db_path`, `n_consolidated`, `n_quarantine`, `n_log_entries`.

---

### `src/export.py` ✅ built

Reads clean and quarantined rows from SQLite and writes a formatted `.xlsx` workbook.
Each sheet has auto-sized columns and a frozen header row. Clean data optionally splits
into per-value sheets by a grouping column (e.g. `region` or `source_file`); the
quarantine sheet is always appended last.

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `read_consolidated(db_path)` | `str \| Path` | DataFrame from `consolidated` table |
| `read_quarantine(db_path)` | `str \| Path` | DataFrame from `quarantine` table |
| `split_by_column(df, col)` | DataFrame, column name | `dict[sheet_name → DataFrame]` |
| `build_sheet_map(clean_df, quarantine_df, group_by)` | DataFrames, optional col name | ordered `dict[sheet_name → DataFrame]` |
| `write_workbook(sheet_map, output_path)` | sheet map dict, path | `ExportResult` |
| `build_summary(result)` | `ExportResult` | plain-English summary string |
| `export(db_path, output_path, group_by)` | paths, optional col name | `ExportResult` (convenience wrapper) |

**Class:** `ExportResult` — dataclass with `output_path`, `sheets`, `n_clean`, `n_quarantine`.

**CLI:** `.venv/bin/python src/export.py [--db data/seed.db] [--output data/output/consolidated.xlsx] [--group-by region]`

---

### `src/report.py` ✅ built

Generates a cleaning summary report for both terminal display and markdown file output.
Reads directly from the three SQLite tables — no DataFrames passed in.

**Report contents:**
- Files processed, rows before/after cleaning
- Columns standardized (count of rename_column log entries)
- Duplicates removed (parsed from cleaning_log)
- Type fixes applied (date normalizations + currency strip counts)
- Full transformation breakdown by type
- Quarantine count, by reason category, and by source file

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `read_consolidated(db_path)` | `str \| Path` | DataFrame from `consolidated` table (drops `id`, `loaded_at`) |
| `read_quarantine(db_path)` | `str \| Path` | DataFrame from `quarantine` table (drops `id`, `quarantined_at`) |
| `read_cleaning_log(db_path)` | `str \| Path` | DataFrame from `cleaning_log` table |
| `generate_cleaning_summary(clean_df, quarantine_df, log_df)` | DataFrames | `CleaningSummary` |
| `generate_quarantine_summary(quarantine_df)` | DataFrame | `QuarantineSummary` |
| `render_terminal(cleaning, quarantine)` | two summary dataclasses | plain-English string (also prints) |
| `render_markdown(cleaning, quarantine, output_path)` | two summary dataclasses, path | `Path` written |
| `report(db_path, output_path, fmt)` | path, optional path, `"markdown"` | `ReportResult` |

**Classes:**
- `CleaningSummary` — `n_files`, `file_names`, `n_rows_before`, `n_rows_after`, `n_columns_standardized`, `n_duplicates_removed`, `n_type_fixes`, `transformation_counts`
- `QuarantineSummary` — `n_quarantined`, `by_reason_type`, `by_source_file`
- `ReportResult` — `terminal_text`, `output_path`

**CLI:** `.venv/bin/python src/report.py [--db data/seed.db] [--output data/output/report.md] [--format markdown]`

---

## `config/`

### `config/validation_rules.yaml` ✅ built

YAML-based rule definitions consumed by `validator.py`. Editing this file is the
only thing needed to adapt the tool to a different dataset — no Python changes
required.

**Sections:**
- `columns` — per-column rules (`type`, `min`, `max`, `required`, `pattern`)
- `min_non_null_fields` — minimum populated fields for a row not to be flagged empty
- `negative_revenue_allowed_files` — list of filenames exempt from `revenue min: 0`

---

## `dashboard/`

### `dashboard/app.py` ✅ built

Streamlit utility dashboard. Reads directly from `data/seed.db` — no pipeline run
required on clone. Functional style: six layers of pure functions with no shared
mutable state. Data is loaded once per session via `@st.cache_data`.

**Pages / views (via `st.tabs`):**
| Tab | Contents |
|-----|----------|
| Overview | Five KPI metric cards (files, total rows, rows passed, rows quarantined, columns standardised) + per-file data quality table with quarantine rate |
| Quarantine | Filterable table of flagged rows — filter by source file and reason category; shows source_file, source_row, quarantine_reason, date, product, region, quantity, revenue |
| Clean Data | Filterable preview of the `consolidated` table — filter by source file, region, and date range |

**Sidebar:** Download `.xlsx` export button (calls `export.py`, serves bytes via `st.download_button`).

**Function layers:**

| Layer | Functions |
|-------|-----------|
| Cached loaders | `load_data`, `load_cleaning_summary`, `load_quarantine_summary` |
| Filter helpers | `filter_quarantine`, `filter_consolidated`, `build_per_file_quality_table` |
| Render functions | `render_kpi_cards`, `render_per_file_quality_table`, `render_quarantine_table`, `render_clean_data_table`, `render_export_button` |
| Page renderers | `render_overview_page`, `render_quarantine_page`, `render_clean_data_page` |
| Entry point | `main()` |

**Run:** `.venv/bin/streamlit run dashboard/app.py`

**Deploy:** Streamlit Community Cloud — point at `dashboard/app.py`, `data/seed.db` ships with the repo.

---

## `data/`

### `data/sample_files/` ✅ built

Eight source files (6 Excel, 2 CSV) with realistic messiness and 24 intentionally
bad rows (~6%). These are the demo inputs — a hiring manager runs the tool against
this folder. See `data/SCHEMA.md` for full per-file documentation.

### `data/SCHEMA.md` ✅ built

Canonical schema, column name mappings per file, per-file messiness descriptions,
bad-row catalog, and reference data (products, prices, reps, customers).

### `data/seed.db` ✅ generated (84 KB — needs Git commit)

Pre-built SQLite database. Generated by running `scripts/run_pipeline.py` against
`data/sample_files/`. Lets the dashboard work immediately on clone without running
the pipeline. 84 KB — well under the 25 MB limit.

**To regenerate:** `.venv/bin/python scripts/run_pipeline.py`

### `data/output/` — gitignored

Export destination for `export.py` and `report.py`. Never committed.

---

## Root Files

### `requirements.txt` ✅ built

Python dependencies, grouped by purpose:
```
# Core pipeline
pandas>=2.0.0        # pd.to_datetime(format="mixed") requires 2.0+
openpyxl>=3.0.0
pyyaml>=6.0
python-dotenv>=1.0.0

# Dashboard
streamlit>=1.30.0    # st.column_config.TextColumn(width=) requires 1.30+

# Google Drive (optional)
google-api-python-client>=2.0.0
google-auth-httplib2>=0.1.0
google-auth-oauthlib>=1.0.0

# Testing
pytest>=7.0.0
```

### `.env.example` ✅ built

Template for Google Drive credentials. Users copy to `.env` and fill in their own
GCP values. The actual `.env` is gitignored.

### `.gitignore` ✅ built

Excludes: `.env`, `credentials.json`, `token.json`, `data/output/`, `.venv/`,
`__pycache__/`, `.DS_Store`.

---

## `scripts/`

### `scripts/run_pipeline.py` ✅ built

End-to-end pipeline runner: `consolidate → validate → load`. The primary entry point
for generating `data/seed.db` and for running the live pipeline against new data.

**Public API:**

| Function | Args | Returns |
|----------|------|---------|
| `resolve_input_folder(source, local_input, folder_id, tmp_dir)` | strings + Path | `Path` to local input folder |
| `run_pipeline(input_folder, db_path, mode, config_path)` | Paths + mode str | `tuple[str, LoadResult]` |

**Usage:**
```bash
# Regenerate data/seed.db from sample files (default)
.venv/bin/python scripts/run_pipeline.py

# Full mode — custom local folder
.venv/bin/python scripts/run_pipeline.py --mode full --input /path/to/folder

# Full mode — Google Drive folder (requires .env credentials)
.venv/bin/python scripts/run_pipeline.py --mode full --source gdrive --folder-id YOUR_FOLDER_ID

# Override the output database path
.venv/bin/python scripts/run_pipeline.py --db data/my_custom.db
```

Prints `validator.summarize()` + `db_loader.build_summary()` output on completion.

---

### `scripts/seed_drive.py` ✅ built

One-time utility to upload the local sample files to a Google Drive folder.
Run this once to set up a live Drive source for testing the `--source gdrive` pipeline mode.

**Behaviour:**
- If `GOOGLE_DRIVE_FOLDER_ID` is set in `.env` (or `--folder-id` is passed), uploads
  all sample files directly to that existing folder — no new folder is created.
- If no folder ID is configured, creates a new folder named `excel_consolidator_samples`
  in the Drive root, then uploads there.

**Usage:**
```bash
# Uses GOOGLE_DRIVE_FOLDER_ID from .env automatically
.venv/bin/python scripts/seed_drive.py

# Override folder at the CLI
.venv/bin/python scripts/seed_drive.py --folder-id <DRIVE_FOLDER_ID>

# Create a new named folder in Drive root
.venv/bin/python scripts/seed_drive.py --folder-name "q1_sales_uploads"
```

Prints the resolved folder ID on completion — paste it as `GOOGLE_DRIVE_FOLDER_ID` in `.env`
if not already set.

---

## Build Status

| File | Status |
|------|--------|
| `src/drive_connector.py` | ✅ built |
| `src/consolidator.py` | ✅ built |
| `src/validator.py` | ✅ built |
| `src/db_loader.py` | ✅ built |
| `src/export.py` | ✅ built |
| `src/report.py` | ✅ built |
| `scripts/run_pipeline.py` | ✅ built |
| `scripts/seed_drive.py` | ✅ built |
| `tests/test_drive_connector.py` | ✅ built |
| `tests/test_consolidator.py` | ✅ built |
| `tests/test_validator.py` | ✅ built |
| `tests/test_db_loader.py` | ✅ built |
| `tests/test_export.py` | ✅ built |
| `tests/test_report.py` | ✅ built (115 tests) |
| `tests/scripts/test_run_pipeline.py` | ✅ built (30 tests) |
| `tests/scripts/test_seed_drive.py` | ✅ built (26 tests) |
| `dashboard/app.py` | ✅ built |
| `.streamlit/config.toml` | ✅ built |
| `config/validation_rules.yaml` | ✅ built |
| `data/sample_files/` | ✅ built |
| `data/seed.db` | ✅ generated (84 KB — needs Git commit) |
| `requirements.txt` | ✅ built |
| `.env.example` | ✅ built |
