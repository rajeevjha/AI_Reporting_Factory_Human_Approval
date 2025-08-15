-- update finance.kyc_gold.ai_sql_candidates set status = 'APPROVED'

-- 00_uc_setup.sql
-- Run as admin in Databricks SQL to create catalog/schemas and staging tables

CREATE CATALOG IF NOT EXISTS finance;

CREATE SCHEMA IF NOT EXISTS finance.kyc_bronze;
CREATE SCHEMA IF NOT EXISTS finance.kyc_silver;
CREATE SCHEMA IF NOT EXISTS finance.kyc_gold;

-- Staging tables for AI approval and exports
CREATE TABLE IF NOT EXISTS finance.kyc_gold.ai_sql_candidates (
  id STRING, report_name STRING, prompt STRING, generated_sql STRING, status STRING,
  created_by STRING, created_at TIMESTAMP, updated_by STRING, updated_at TIMESTAMP, 
  reviewed_by STRING, reviewed_at TIMESTAMP, published_at TIMESTAMP,notes STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS finance.kyc_gold.report_candidates (
    id STRING,
    report_name STRING,
    dataset_view STRING,
    chart_type STRING,
    filters STRING,
    owner STRING,
    status STRING,  -- pending, approved, rejected, published
    dashboard_id STRING,
    report_url STRING,
    submitted_at TIMESTAMP,
    decision_at TIMESTAMP,
    published_at TIMESTAMP
) USING DELTA;