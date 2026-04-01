# Sample Files Schema

Row counts were sampled from **Poisson(λ=50)** with seed 42. Re-run
`data/sample_files/generate_samples.py` to regenerate all files with the same
row counts.

---

## Summary Table

| File | Format | Rows | Bad Rows | Period | Regions |
|------|--------|-----:|--------:|--------|---------|
| Q1_2024_sales.xlsx | XLSX | 47 | 3 | Q1 2024 | West, East, Central |
| Q2_2024_sales.xlsx | XLSX | 55 | 2 | Q2 2024 | West, East, Central |
| Q3_2024_sales.xlsx | XLSX | 42 | 5 | Q3 2024 | West, East, Central |
| Q4_2024_sales.xlsx | XLSX | 52 | 4 | Q4 2024 | West, East, Central |
| west_region_2024.xlsx | XLSX | 58 + 2 title rows | 2 | FY 2024 | West only |
| east_central_Q2_Q3.xlsx | XLSX | 43 | 3 | Q2–Q3 2024 | East, Central |
| pipeline_q3_q4.csv | CSV | 46 | 2 | Q3–Q4 2024 | West, East, Central |
| returns_flagged.csv | CSV | 49 | 3 | Q1–Q3 2024 | West, East, Central |

**Total:** 392 data rows across 8 files, 24 intentionally bad rows (~6%).

---

## Canonical Schema

After consolidation all files should map to these columns:

| Canonical Column | Type | Description |
|------------------|------|-------------|
| `date` | DATE | Transaction date (YYYY-MM-DD after parsing) |
| `product` | TEXT | Product name |
| `region` | TEXT | Sales region: West / East / Central |
| `sales_rep` | TEXT | Full name of sales representative |
| `customer` | TEXT | Customer / account name |
| `quantity` | INTEGER | Units sold (must be > 0) |
| `revenue` | REAL | Transaction revenue in USD (must be ≥ 0 for sales) |
| `source_file` | TEXT | Originating filename (added by consolidator) |
| `source_row` | INTEGER | 1-based row index in source file (added by consolidator) |

---

## Column Name Mapping

| Canonical | Q1 | Q2 | Q3 | Q4 | west | east_central | pipeline.csv | returns.csv |
|-----------|----|----|----|----|------|--------------|--------------|-------------|
| `date` | Date | transaction_date | Date | date | Transaction Date | Sale Date | date | Return Date |
| `product` | Product | product_name | Item | product | Product Line | Product | product | Product Name |
| `region` | Region | territory | Area | region | *(implicit: West)* | Region | region | Region |
| `sales_rep` | Sales Rep | rep | Salesperson | sales_rep | Rep Name | Rep | sales_rep | Sales Rep |
| `customer` | Customer | client | Account | customer | Client Name | Customer | customer | Customer |
| `quantity` | Qty | units | Quantity | quantity | Units Sold | Qty | quantity | Units |
| `revenue` | Revenue | Rev. | Total Revenue | amount | Revenue ($) | $ | revenue | Refund Amt |

---

## File Descriptions

### Q1_2024_sales.xlsx

**Sheet:** `Q1 Sales`
**Date format:** ISO (`YYYY-MM-DD`)
**Rep name style:** Full name (`Dana Lee`)
**Messiness:** Minimal — this is the "clean baseline" file.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `Date` | string | ISO format |
| product | `Product` | string | |
| region | `Region` | string | |
| sales_rep | `Sales Rep` | string | |
| customer | `Customer` | string | |
| quantity | `Qty` | integer | |
| revenue | `Revenue` | float | |

**Bad rows (3):**
- `Revenue = -450.00` — negative revenue
- `Customer = NULL` — missing required field
- All-NULL row — entirely empty

---

### Q2_2024_sales.xlsx

**Sheet:** `Sheet1`
**Date format:** Mixed — US (`MM/DD/YYYY`), ISO, and written (`Month D, YYYY`) randomly assigned
**Rep name style:** Abbreviated (`D. Lee`)
**Messiness:** Renamed columns, abbreviated rep names, three different date formats in one file.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `transaction_date` | string | 3 mixed formats |
| product | `product_name` | string | |
| region | `territory` | string | |
| sales_rep | `rep` | string | abbreviated |
| customer | `client` | string | |
| quantity | `units` | integer | |
| revenue | `Rev.` | mixed | usually float |

**Bad rows (2):**
- Comment row: `"Mike please update these numbers before EOD"` in date column
- `Rev. = "pending"` — text in numeric column

---

### Q3_2024_sales.xlsx

**Sheet:** `Q3`
**Date format:** ISO (`YYYY-MM-DD`)
**Rep name style:** Full name
**Messiness:** Most heavily renamed columns. Most bad-row variety of any file.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `Date` | string | |
| product | `Item` | string | |
| region | `Area` | string | |
| sales_rep | `Salesperson` | string | |
| customer | `Account` | string | |
| quantity | `Quantity` | mixed | |
| revenue | `Total Revenue` | float | |

**Bad rows (5):**
- `Date = "2024-13-01"` — month 13, impossible date
- `Quantity = 0, Revenue = 0.00` — zero-quantity transaction
- `Quantity = "TBD"` — text in numeric column
- `Revenue = -880.00` — negative revenue
- `Date = "2024-02-30"` — February 30, impossible date

---

### Q4_2024_sales.xlsx

**Sheet:** `Q4 Data`
**Date format:** Written month names (`October 1, 2024` or `Oct 1, 2024`) throughout
**Rep name style:** Full name
**Messiness:** Non-parseable date format across all rows, duplicate record, future date outlier.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `date` | string | written month, not parseable as-is |
| product | `product` | string | |
| region | `region` | string | |
| sales_rep | `sales_rep` | string | |
| customer | `customer` | string | |
| quantity | `quantity` | integer | |
| revenue | `amount` | mixed | usually float |

**Bad rows (4):**
- `date = "2099-01-01"` — far-future date (outlier / data entry error)
- Two identical rows for `"November 19, 2024" / Data Suite / East` — exact duplicate
- `amount = "N/A"` — text in numeric column

---

### west_region_2024.xlsx

**Sheet:** `West Region`
**Date format:** ISO (`YYYY-MM-DD`)
**Rep name style:** Full name
**Messiness:** Extra title row + blank spacer before headers (row 1 = title, row 2 = blank, row 3 = headers). No Region column — region is implied (West-only file).

**File structure:**
```
Row 1: "West Region Sales Summary — FY 2024"   ← title, must be skipped
Row 2: (blank)                                  ← spacer, must be skipped
Row 3: Transaction Date | Product Line | ...    ← actual headers
Row 4+: data
```

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `Transaction Date` | string | ISO format |
| product | `Product Line` | string | |
| region | *(none)* | — | must be injected as "West" |
| sales_rep | `Rep Name` | string | |
| customer | `Client Name` | string | |
| quantity | `Units Sold` | integer | |
| revenue | `Revenue ($)` | float | |

**Bad rows (2):**
- `Revenue ($) = -2200.00` — negative revenue
- Comment row: `"TODO: Dana confirm Q4 numbers with finance"` in date column

---

### east_central_Q2_Q3.xlsx

**Sheet:** `Sales`
**Date format:** ISO (`YYYY-MM-DD`)
**Rep name style:** Full name
**Messiness:** Single-character revenue header (`$`), section-divider row mid-file, currency-formatted string in revenue.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `Sale Date` | string | |
| product | `Product` | string | |
| region | `Region` | string | East or Central only |
| sales_rep | `Rep` | string | |
| customer | `Customer` | string | |
| quantity | `Qty` | integer | |
| revenue | `$` | mixed | usually float |

**Bad rows (3):**
- Section-divider: `"--- Q3 below ---"` in date column
- `$ = "$3,000"` — currency string with symbol and comma (not a float)
- `Rep = NULL, Customer = NULL` — two required fields missing on one row

---

### pipeline_q3_q4.csv

**Format:** CSV (comma-separated)
**Date format:** ISO (`YYYY-MM-DD`)
**Rep name style:** Full name
**Messiness:** Minimal — this is the "clean CSV" baseline. Spans two quarters.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `date` | string | ISO format |
| product | `product` | string | |
| region | `region` | string | |
| sales_rep | `sales_rep` | string | |
| customer | `customer` | string | |
| quantity | `quantity` | mixed | usually integer |
| revenue | `revenue` | float | |

**Bad rows (2):**
- `revenue = -1500.00` — negative revenue
- `quantity = "unknown"` — text in numeric column

---

### returns_flagged.csv

**Format:** CSV (comma-separated)
**Date format:** US (`MM/DD/YYYY`)
**Rep name style:** Full name
**Domain:** Return/refund records. **Negative `Refund Amt` is expected and correct** for all clean rows. The consolidator does not know this file is returns-only (no transaction-type column), so validation rules will flag all negative amounts unless the file type is handled separately.

| Column | Raw Name | Type | Notes |
|--------|----------|------|-------|
| date | `Return Date` | string | US format MM/DD/YYYY |
| product | `Product Name` | string | |
| region | `Region` | string | |
| sales_rep | `Sales Rep` | string | |
| customer | `Customer` | string | |
| quantity | `Units` | integer | units returned |
| revenue | `Refund Amt` | float | **negative values are valid here** |

**Bad rows (3):**
- Comment row: `"Finance: do not process until dispute resolved"` in date column
- `Refund Amt = ""` — missing refund amount (empty string)
- `Return Date = "13/01/2024"` — month 13, impossible date

---

## Reference Data

**Products and base unit prices:**

| Product | Unit Price (USD) |
|---------|----------------:|
| Analytics Pro | 1,500 |
| Data Suite | 2,200 |
| Reporting Pkg | 1,450 |
| Insights Hub | 1,800 |
| DataViz Pro | 2,500 |

**Sales reps by region:**

| Region | Rep |
|--------|-----|
| West | Dana Lee |
| East | Tom Rivera |
| Central | Sara Kim |

**Customers:** Acme Corp, Globex LLC, Initech, Umbrella Inc, Soylent Corp, Massive Dynamics, Wayne Enterprises, Oscorp
