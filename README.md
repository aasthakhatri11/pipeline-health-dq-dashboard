# Pipeline Health & Data Quality Dashboard

Capstone Topic #30 - Databricks & Snowflake (Excelr x KIIT). A data-quality "watcher" built on a simulated e-commerce order pipeline: seven named validation rules run on every load, a freshness check measures how stale each table is, the pipeline runs on a schedule, and a live dashboard shows the results from Snowflake.

**Live dashboard:** https://pipeline-health-dq-dashboard-g5htxaegzm5kjvukxkgwcq.streamlit.app/

## What it does

| Stage | Where | What happens |
|---|---|---|
| Generate | Databricks | A seeded generator (seed 30) writes 10 loaded order files, 10 customer files and 6 new order files. The 6 new files carry 500 planted defects, so every rule has a known expected count. |
| Bronze | Databricks | Every file lands as strings with `_source_file`, `_ingested_at`, `_load_id`, `_row_hash`, plus `bronze_load_manifest` (one row per file, including the empty one). |
| Silver | Databricks | `try_cast`, dedupe against the target table, rules R1-R5 (row scope) and R6-R7 (file scope). `silver_orders` = 74,500 rows, `silver_rejects` = 500 rows. |
| Gold | Databricks | `gold_dq_results` (132 rows) and `gold_freshness` (2 rows). |
| Export and load | Databricks to Snowflake | Gold to CSV in a Volume, then a Snowflake stage, then `COPY INTO`. A second `COPY INTO` loads 0 files (idempotent). |
| Schedule | Databricks Jobs | The notebook runs weekly as a serverless Job. |
| Dashboard | Streamlit Community Cloud | Reads Snowflake live and answers the three Gold-layer questions. |

## The seven rules

| Rule | Scope | Catches | Expected fires |
|---|---|---|---|
| R1 null_key | row | missing order_id or customer_id | 0 |
| R2 duplicate_order_id | row | order_id already in silver_orders (checked against the target table, not the batch) | 150 (day_13) |
| R3 quantity_not_numeric | row | quantity that fails `try_cast` to INT | 40 (day_12) |
| R4 unknown_customer | row | customer_id missing from the customer master | 220 (day_16) |
| R5 order_ts_out_of_range | row | date outside 2024-01-01 to today | 90 (day_16) |
| R6 column_contract | file | arrived column list differs from the contract (ordered comparison) | 1 file (day_14) |
| R7 row_count_vs_median | file | row count more than 40% away from the median of the last 7 loads | 1 file (day_15) |

## Repository contents

| File | Purpose |
|---|---|
| `01_pipeline_bronze_silver_gold.ipynb` | Generator, Bronze, Silver, Gold, export. This is the notebook the Databricks Job runs. |
| `sql/01_setup_and_load.sql` | Snowflake: database, schema, tables, stage, `COPY INTO`. |
| `sql/02_queries.sql` | Snowflake: row-count checks and the rule-concentration query. |
| `streamlit_app.py` | The dashboard. |
| `requirements.txt` | Dashboard dependencies. |
| `secrets_template.toml` | Shape of the Streamlit secrets file (no real credentials). |

## How to run it

**1. Databricks.** Import the notebook. Set `MY_ID` in the first cell. Run all cells, or wrap the notebook in a Job with a schedule. Run the Job or notebook twice: results must not change.

**2. Snowflake.** Download the CSVs written to `.../export/` in your Volume (`gold_dq_results`, `gold_freshness`, `gold_load_manifest`). Upload them to the stage and run `sql/01_setup_and_load.sql`. Run `COPY INTO` a second time: it must report 0 files loaded. Then run `sql/02_queries.sql`.

**3. Dashboard (local).**
```bash
pip install -r requirements.txt
mkdir -p .streamlit && cp secrets_template.toml .streamlit/secrets.toml   # then fill in your values
streamlit run streamlit_app.py
```

**4. Dashboard (Streamlit Community Cloud).** Deploy the repo, then paste the contents of your secrets file into the app's *Secrets* settings. Never commit real credentials: `.gitignore` excludes `.streamlit/secrets.toml`.

## Verified results

| Check | Expected | Result |
|---|---|---|
| Bronze orders / customers / manifest | 75,000 / 3,000 / 26 | matched |
| New files (day_11 to day_16) | 5000, 5000, 5000, 5000, 0, 5000 | matched |
| Rejects R2 / R3 / R4 / R5 | 150 / 40 / 220 / 90 (500 total) | matched |
| silver_orders / silver_rejects | 74,500 / 500 | matched |
| gold_dq_results | 132 rows | matched |
| Freshness | orders 0 h pass, customers 144 h fail | matched |
| Second `COPY INTO` | 0 files | matched |
| Scheduled Job run | succeeds, launched by scheduler | 5m 17s |

## Design decisions worth knowing

- **`try_cast` and `try_divide`, never `cast` or `/`.** Databricks Free Edition runs in ANSI mode, where a bad cast or a 0/0 division stops the cell.
- **Duplicates are checked against the target table.** `dropDuplicates` on day_13 alone finds nothing, because its 150 repeats collide with day_01.
- **File-scope rules read the manifest.** The empty file (day_15) passes every row rule as 0 of 0, so only R7 can see it.
- **Freshness uses business dates.** Using `_ingested_at` would make every table look minutes old.
- **`load_id` is a fresh UUID on every notebook run.** Export `gold_dq_results`, `gold_freshness` and `gold_load_manifest` from the same run, and reload all three together, or the dashboard labels will not match.
- **Alarm.** The last notebook cell raises an exception when any rule fails or a table is stale, so the Job run turns red. Set `RAISE_ON_FAIL = False` to disable it.

## Source specification

Dr. Kanthi Kiran Sirra, datatrends.tech - Capstone Project Briefs, Topic #30.
