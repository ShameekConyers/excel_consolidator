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
                        (Excel out)   (HTML report)   (Streamlit UI)
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

### `src/validator.py` 🔲 planned

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

**Planned interface:**
```python
validate(df: pd.DataFrame, rules_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]
# returns (clean_df, quarantine_df)
```

---

### `src/db_loader.py` 🔲 planned

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

**Planned interface:**
```python
load(clean_df, quarantine_df, log_entries, db_path)
```

**CLI:** `.venv/bin/python src/db_loader.py [--seed | --full] [--db path/to/output.db]`

---

### `src/export.py` 🔲 planned

Exports the clean consolidated data to a single `.xlsx` file. Optionally splits into
multiple sheets by a grouping column (e.g. by `source_file` or `region`).

Also exports the quarantine table to a separate sheet so the data owner can review
and fix flagged rows in their source files.

**CLI:** `.venv/bin/python src/export.py --output data/output/consolidated.xlsx`

---

### `src/report.py` 🔲 planned

Generates a cleaning summary report in both terminal text and HTML.

**Report contents:**
- Files processed, total rows ingested
- Rows passed vs. quarantined (overall and per file)
- Columns standardized (before → after name mapping)
- Duplicates removed (count and source files)
- Quarantine breakdown by reason type

**CLI:** `.venv/bin/python src/report.py [--html data/output/report.html]`

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

### `dashboard/app.py` 🔲 planned

Streamlit utility dashboard. Reads directly from `data/seed.db` — no pipeline run
required on clone.

**Pages / views:**
| View | Contents |
|------|----------|
| Summary | KPI cards: files processed, rows loaded, rows quarantined, columns standardized |
| Data Quality | Per-file table: row counts, issues found, quarantine count |
| Quarantine | Filterable table of flagged rows (source file, row #, reason, original values) |
| Clean Data | Filterable preview of the `consolidated` table |
| Export | Download consolidated Excel file directly from UI |

**Run:** `.venv/bin/streamlit run dashboard/app.py`

---

## `data/`

### `data/sample_files/` ✅ built

Eight source files (6 Excel, 2 CSV) with realistic messiness and 24 intentionally
bad rows (~6%). These are the demo inputs — a hiring manager runs the tool against
this folder. See `data/SCHEMA.md` for full per-file documentation.

### `data/SCHEMA.md` ✅ built

Canonical schema, column name mappings per file, per-file messiness descriptions,
bad-row catalog, and reference data (products, prices, reps, customers).

### `data/seed.db` 🔲 generated by `db_loader.py`

Pre-built SQLite database committed to Git. Lets the dashboard work immediately on
clone without running the pipeline. Under 25 MB.

### `data/output/` — gitignored

Export destination for `export.py` and `report.py`. Never committed.

---

## Root Files

### `requirements.txt` ✅ built

Python dependencies. Current contents:
```
google-api-python-client>=2.0.0
google-auth-httplib2>=0.1.0
google-auth-oauthlib>=1.0.0
python-dotenv>=1.0.0
pytest>=7.0.0
pandas>=2.0.0
openpyxl>=3.0.0
pyyaml>=6.0
```
Still needed: `streamlit` (for `dashboard/app.py`). Note: pandas `>=2.0.0` is required — `pd.to_datetime(format="mixed")` was introduced in 2.0.

### `.env.example` ✅ built

Template for Google Drive credentials. Users copy to `.env` and fill in their own
GCP values. The actual `.env` is gitignored.

### `.gitignore` ✅ built

Excludes: `.env`, `credentials.json`, `token.json`, `data/output/`, `.venv/`,
`__pycache__/`, `.DS_Store`.

---

## `scripts/`

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
| `scripts/seed_drive.py` | ✅ built |
| `src/consolidator.py` | ✅ built |
| `tests/test_drive_connector.py` | ✅ built |
| `src/validator.py` | 🔲 next — load YAML rules, split df into clean + quarantine |
| `tests/test_consolidator.py` | 🔲 next — unit tests for all 9 public functions |
| `src/db_loader.py` | 🔲 after validator |
| `src/export.py` | 🔲 after db_loader |
| `src/report.py` | 🔲 after db_loader |
| `tests/test_validator.py` | 🔲 after validator |
| `dashboard/app.py` | 🔲 last |
| `config/validation_rules.yaml` | ✅ built |
| `data/sample_files/` | ✅ built |
| `data/seed.db` | 🔲 needs db_loader.py |
| `requirements.txt` | ✅ built |
| `.env.example` | ✅ built |
