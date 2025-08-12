-- 00_uc_setup.sql
-- Run as admin in Databricks SQL to create catalog/schemas and staging tables

CREATE CATALOG IF NOT EXISTS finance;

CREATE SCHEMA IF NOT EXISTS finance.kyc_bronze;
CREATE SCHEMA IF NOT EXISTS finance.kyc_silver;
CREATE SCHEMA IF NOT EXISTS finance.kyc_gold;

-- Staging tables for AI approval and exports
CREATE TABLE IF NOT EXISTS finance.kyc_gold.ai_sql_candidates (
  id STRING, report_name STRING, prompt STRING, generated_sql STRING, status STRING,
  created_by STRING, created_at TIMESTAMP, updated_by STRING, updated_at TIMESTAMP, notes STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS finance.kyc_gold.ai_sql_approval_log (
  id STRING, report_name STRING, user STRING, decision STRING, sql_text STRING, notes STRING, ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS finance.kyc_gold.report_export_queue (
  id STRING, report_name STRING, view_full_name STRING, status STRING, created_at TIMESTAMP,
  finished_at TIMESTAMP, export_path STRING, notify_emails ARRAY<STRING>
) USING DELTA;
