# Drive-to-Database: Automated Excel Consolidation Pipeline

## Overview

This tool takes a folder of messy Excel files — or a Google Drive folder — and ingests them into a SQLite database. During ingestion it tries to standardize and clean the data as much as it can. Any rows it can't fix get offloaded to a quarantine table with a plain-English explanation of what went wrong. From there a Streamlit dashboard shows you the full picture: what loaded cleanly, what needs review, and why.

---

## Motivation

This is built for small businesses or ad hoc projects where a few people are collaborating through spreadsheets rather than a proper data warehouse. Maybe they're not familiar with PowerBI, or they just prefer working in Google Drive because it's easy to share. I've seen this pattern in a few real projects — a shared Drive folder with 20-30 Excel files that the team manually updates, with inconsistent column names and dates formatted six different ways.

The result is usually someone manually cleaning everything in their own spreadsheet before they can run any analysis. This tool automates that cleanup and gives you a proper database at the end, plus an honest report of everything it touched and everything it couldn't fix.

---

## Quick Start — Local Mode (no credentials needed)

```bash
git clone <repo>
cd excel_consolidator
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

.venv/bin/python src/consolidator.py --input data/sample_files/
.venv/bin/python src/db_loader.py
.venv/bin/streamlit run dashboard/app.py
```

The repo ships with sample Excel files in `data/sample_files/` that have realistic messiness built in: inconsistent column headers, mixed date formats, duplicates across files, and rows of bad data that trigger the quarantine system. No credentials or external dependencies needed.

---

## Google Drive Mode (optional)

To pull files directly from a Drive folder instead:

**1. Set up credentials**

Follow the [Google Drive API Python Quickstart](https://developers.google.com/drive/api/quickstart/python) to create OAuth 2.0 credentials and download `credentials.json` to the project root.

**2. Upload sample files to Drive**

```bash
cp .env.example .env
.venv/bin/python scripts/seed_drive.py
```

This creates a Drive folder, uploads the sample files, and prints the folder ID. Paste it as `GOOGLE_DRIVE_FOLDER_ID` in your `.env`.

Or target an existing folder:
```bash
.venv/bin/python scripts/seed_drive.py --folder-id YOUR_FOLDER_ID
```

**3. Run the pipeline**

```bash
.venv/bin/python src/consolidator.py --source gdrive --folder-id YOUR_FOLDER_ID
.venv/bin/python src/db_loader.py --full
.venv/bin/streamlit run dashboard/app.py
```

The integration code is in `src/drive_connector.py`. Secrets stay in `.env` and never touch the repo.

---

## Quarantine System

Bad data is never silently dropped. Every row that fails validation goes into a `quarantine` table with the original data unchanged, the source file and row number, and a plain-English explanation of what went wrong.

Some examples of what that looks like:
```
"revenue is negative (-450) in row 23 of Q3_sales.xlsx"
"date '2099-13-45' is not a valid date in row 7 of Jan_report.xlsx"
"required field 'quantity' is empty in row 51 of inventory.xlsx"
"value 'see notes below' cannot be converted to numeric in row 12 of Feb_data.xlsx"
```

The dashboard surfaces these in a filterable table — the output you hand back to whoever owns the source files.

---

## Validation Rules

Rules live in `config/validation_rules.yaml` rather than hardcoded in Python. To adapt the tool to a different dataset, just edit the config:

```yaml
columns:
  revenue:
    type: numeric
    min: 0
    required: true
  date:
    type: date
    min: "2015-01-01"
    max: "2026-12-31"
    required: true
  email:
    type: text
    pattern: ".*@.*\\..*"
    required: false

min_non_null_fields: 3
flag_non_conforming_types: true
```

---

## What It Handles

| Issue | How it's handled |
|-------|-----------------|
| Inconsistent column names (`Revenue`, `Rev.`, `Total Revenue`) | Mapped to canonical names via configurable alias dict |
| Mixed date formats (`2024-01-15`, `01/15/2024`, `Jan 15 2024`) | Normalized to ISO 8601 |
| Numbers stored as text, currency symbols in numeric columns | Stripped and cast; failures quarantined |
| Duplicates across files | First occurrence kept, duplicates logged |
| Multi-sheet workbooks | First sheet with data used; configurable |
| Empty rows, comment rows | Flagged and quarantined with reason |

---

## Architecture

```
excel_consolidator/
├── src/
│   ├── consolidator.py         # Discovers, reads, standardizes, and merges Excel files
│   ├── validator.py            # Routes rows to clean_df or quarantine_df with reasons
│   ├── drive_connector.py      # Optional: downloads files from Google Drive
│   ├── db_loader.py            # Loads both tables into SQLite (--seed / --full)
│   ├── export.py               # Exports consolidated clean data to Excel
│   └── report.py               # Generates terminal + HTML cleaning summary
├── config/
│   └── validation_rules.yaml   # All validation rules — edit here, not in Python
├── dashboard/
│   └── app.py                  # Streamlit utility dashboard
├── data/
│   ├── sample_files/           # 8 messy Excel/CSV files committed to repo
│   ├── seed.db                 # Pre-built SQLite DB from sample files
│   └── output/                 # Consolidated exports (gitignored)
├── docs/
│   └── architecture.md         # File-by-file technical reference
├── scripts/
│   └── seed_drive.py           # One-time utility: upload sample files to Google Drive
├── tests/
│   ├── test_drive_connector.py
│   └── test_consolidator.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

**Database tables:**
- `consolidated` — clean rows that passed all validation rules
- `quarantine` — failed rows with original data + source info + plain-English reason
- `cleaning_log` — record of every transformation applied (column renames, type conversions, duplicate removals)

---

## Export Commands

```bash
# Export consolidated clean data to Excel
.venv/bin/python src/export.py --output data/output/consolidated.xlsx

# Generate cleaning summary report
.venv/bin/python src/report.py
```

---

## Dashboard

> *Screenshots to be added.*

**Views:**
- **Summary** — KPI cards: files processed, rows loaded, rows quarantined, columns standardized
- **Data Quality Report** — per-file breakdown of issues found
- **Quarantine** — filterable table of flagged rows with source, row number, and reason
- **Clean Data Preview** — filterable view of the consolidated dataset
- **Export** — download consolidated Excel directly from the UI

---

## Tools

| Category | Tools |
|----------|-------|
| File processing | Python, pandas, openpyxl |
| Database | SQLite |
| Validation config | PyYAML |
| Google Drive integration | `google-api-python-client`, `google-auth-oauthlib` |
| Dashboard | Streamlit |
| Environment | `python-dotenv` |
