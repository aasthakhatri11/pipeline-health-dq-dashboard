-- 01_setup_and_load.sql
-- Snowflake: database, schema, tables, and load from the internal stage.
-- Run top to bottom. CREATE OR REPLACE empties the tables, so this is safe to re-run.

CREATE DATABASE IF NOT EXISTS capstone30;
CREATE SCHEMA IF NOT EXISTS capstone30.pipeline_dq;
USE SCHEMA capstone30.pipeline_dq;

CREATE OR REPLACE TABLE gold_dq_results (
    table_name   STRING,
    load_id      STRING,
    rule_id      STRING,
    rule_name    STRING,
    rule_scope   STRING,
    rows_checked INT,
    rows_failed  INT,
    fail_rate    FLOAT,
    rule_status  STRING
);

CREATE OR REPLACE TABLE gold_freshness (
    table_name              STRING,
    check_ts                DATE,
    last_business_ts        DATE,
    hours_behind            INT,
    expected_interval_hours INT,
    freshness_status        STRING
);

-- Lookup used by the dashboard: load_id -> file name and business date
CREATE OR REPLACE TABLE gold_load_manifest (
    load_id       STRING,
    table_name    STRING,
    load_label    STRING,
    business_date DATE,
    rows_in_file  NUMBER
);

-- CREATE OR REPLACE STAGE gold_stage;  -- created once already, do not re-run
-- Upload the three CSVs from Databricks (export/ folder) with:
-- Snowsight > Ingestion > Load files into a Stage > CAPSTONE30.PIPELINE_DQ.GOLD_STAGE
LIST @gold_stage;

-- Spark names each export file part-00000-tid-<unique number>-....csv, so each COPY
-- picks its file by the unique number. Copy the number from the LIST output above.
-- All three files must come from the SAME notebook run (load_id changes on every run).
COPY INTO gold_freshness FROM @gold_stage
  PATTERN = '.*2835747697007658351.*'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO gold_dq_results FROM @gold_stage
  PATTERN = '.*8135349591552332656.*'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO gold_load_manifest FROM @gold_stage
  PATTERN = '.*897839587313685376.*'
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Idempotency proof: run the three COPY INTO statements above a second time.
-- Each must report 0 files loaded (Snowflake load metadata remembers the files).
