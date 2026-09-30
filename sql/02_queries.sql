-- 02_queries.sql
-- Snowflake: verification checks and the analysis queries behind the dashboard.
USE SCHEMA capstone30.pipeline_dq;

-- Row counts: expect 132 / 2 / 26
SELECT 'GOLD_DQ_RESULTS' AS tbl, COUNT(*) AS n FROM gold_dq_results
UNION ALL SELECT 'GOLD_FRESHNESS', COUNT(*) FROM gold_freshness
UNION ALL SELECT 'GOLD_LOAD_MANIFEST', COUNT(*) FROM gold_load_manifest;

-- Every failing rule must carry a readable load label (expect 6 rows, no empty labels)
SELECT d.load_id, m.load_label, d.rule_id, d.rows_failed
FROM gold_dq_results d
LEFT JOIN gold_load_manifest m ON d.load_id = m.load_id
WHERE d.rows_failed > 0
ORDER BY m.load_label;

-- Q3: which rule fires most, and is it one broken file or the data itself?
-- concentration = worst single load / total failures (1.000 = all from one load)
SELECT rule_id, rule_name,
       SUM(rows_failed)          AS total_failed,
       COUNT_IF(rows_failed > 0) AS loads_fired,
       COUNT(*)                  AS loads_checked,
       MAX(rows_failed)          AS worst_single_load,
       ROUND(MAX(rows_failed) / NULLIF(SUM(rows_failed), 0), 3) AS concentration,
       ROUND(SUM(rows_failed) / NULLIF(SUM(rows_checked), 0), 5) AS fail_rate
FROM gold_dq_results
WHERE rule_scope = 'row' AND table_name = 'orders'
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Q2: how stale is each table?
SELECT table_name, last_business_ts, hours_behind, expected_interval_hours, freshness_status
FROM gold_freshness;
