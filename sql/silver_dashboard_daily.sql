-- Databricks notebook source
-- DBTITLE 1,Silver layer: GA4 dashboard daily
-- MAGIC %md
-- MAGIC ## Silver layer: GA4 dashboard daily
-- MAGIC
-- MAGIC Reads from `dashboard_analytics_raw` and produces a clean, enriched silver table in `dashboard_analytics_silver`.
-- MAGIC
-- MAGIC Transformations applied:
-- MAGIC - Join daily metrics to property metadata (`account_name`, `property_name`)
-- MAGIC - Cast metric columns from DOUBLE to BIGINT
-- MAGIC - Rename all columns to snake_case
-- MAGIC - Drop rows with NULL dates or NULL metrics

-- COMMAND ----------

-- DBTITLE 1,Create silver schema
CREATE SCHEMA IF NOT EXISTS catalog_40_copper_statistics_services.dashboard_analytics_silver;

-- COMMAND ----------

-- DBTITLE 1,Build silver table
CREATE OR REPLACE TABLE catalog_40_copper_statistics_services.dashboard_analytics_silver.ga4_dashboard_daily AS
SELECT
  d.property_id,
  p.account_name,
  p.accountId   AS account_id,
  p.property_name,
  d.date,
  CAST(d.totalUsers       AS BIGINT) AS total_users,
  CAST(d.screenPageViews  AS BIGINT) AS screen_page_views,
  CAST(d.sessions         AS BIGINT) AS sessions
FROM catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_daily d
LEFT JOIN catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties p
  ON d.property_id = p.propertyId
WHERE
  d.date            IS NOT NULL
  AND d.totalUsers       IS NOT NULL
  AND d.screenPageViews  IS NOT NULL
  AND d.sessions         IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,Quality check: null and duplicate audit
-- Rows where property enrichment failed (no matching property found)
SELECT
  'unmatched_properties' AS check_name,
  COUNT(*) AS row_count
FROM catalog_40_copper_statistics_services.dashboard_analytics_silver.ga4_dashboard_daily
WHERE property_name IS NULL

UNION ALL

-- Duplicate (property_id, date) combinations
SELECT
  'duplicate_property_date_combinations' AS check_name,
  COUNT(*) AS row_count
FROM (
  SELECT property_id, date
  FROM catalog_40_copper_statistics_services.dashboard_analytics_silver.ga4_dashboard_daily
  GROUP BY property_id, date
  HAVING COUNT(*) > 1
)

UNION ALL

-- Rows with zero or negative metrics (likely bad data)
SELECT
  'non_positive_metrics' AS check_name,
  COUNT(*) AS row_count
FROM catalog_40_copper_statistics_services.dashboard_analytics_silver.ga4_dashboard_daily
WHERE total_users <= 0 OR screen_page_views <= 0 OR sessions <= 0;

-- COMMAND ----------

-- DBTITLE 1,Summary: row counts and date range
SELECT
  COUNT(*)                        AS total_rows,
  COUNT(DISTINCT property_id)     AS distinct_properties,
  MIN(date)                       AS earliest_date,
  MAX(date)                       AS latest_date,
  COUNT(DISTINCT account_name)    AS distinct_accounts
FROM catalog_40_copper_statistics_services.dashboard_analytics_silver.ga4_dashboard_daily;
