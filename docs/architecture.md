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

### `src/consolidator.py` 🔲 planned

Core ingestion and standardization module. Accepts a folder path (local or a temp
folder populated by `drive_connector.py`) and produces a single merged DataFrame.

**Responsibilities:**
- Discover all `.xlsx`, `.xls`, `.csv` files in the input folder
- Read each file, auto-detect which sheet contains data (skips title/blank rows)
- Rename columns to canonical names using the alias map in `SCHEMA.md`
- Inject a `region` column for files that don't have one (e.g. `west_region_2024.xlsx`)
- Parse and normalize all date columns to ISO 8601 (`YYYY-MM-DD`)
- Strip currency symbols and commas from numeric columns; cast to float
- Attach `source_file` and `source_row` (1-based) columns to every row
- Detect and remove exact duplicate rows across files; log each removal
- Write every transformation to the `cleaning_log`

**Planned interface:**
```python
consolidate(input_dir: str | Path) -> tuple[pd.DataFrame, list[dict]]
# returns (merged_df, cleaning_log_entries)
```

**CLI:** `.venv/bin/python src/consolidator.py --input data/sample_files/`

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
```
Will grow as `consolidator.py` (pandas, openpyxl), `db_loader.py`, and
`dashboard/app.py` (streamlit) are added.

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
| `src/consolidator.py` | 🔲 planned |
| `src/validator.py` | 🔲 planned |
| `src/db_loader.py` | 🔲 planned |
| `src/export.py` | 🔲 planned |
| `src/report.py` | 🔲 planned |
| `dashboard/app.py` | 🔲 planned |
| `config/validation_rules.yaml` | ✅ built |
| `data/sample_files/` | ✅ built |
| `data/seed.db` | 🔲 needs db_loader.py |
| `requirements.txt` | ✅ built (partial) |
| `.env.example` | ✅ built |
