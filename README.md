Reporting Factory (Medallion) - Package

Contents:
- notebooks/: Bronze, Silver, Gold ETL + AI generator + export worker
- app/: Streamlit approval app (05_human_approval_app.py)
- metadata/: report_definitions.json
- data/: paysim.csv (sample)
- sql/: example saved queries + 00_uc_setup.sql
- dashboards/: placeholders
- jobs/: job template

Quickstart:
1) Upload data/paysim.csv to DBFS:
   %fs cp file:/local/path/paysim.csv dbfs:/FileStore/paysim.csv

2) Run the UC setup SQL as an admin:
   -- open SQL editor and run the SQL in sql/00_uc_setup.sql

3) Put notebooks into a Databricks Repo and attach to a cluster.

4) Install cluster libraries: streamlit, openai, pandas

5) Create secret scope 'llm' and add 'openai_api_key'

6) Run notebooks in order:
   01_bronze_ingestion -> 02_silver_transformation -> 03_gold_enrichment -> 04_ai_sql_generator

7) Launch Streamlit app using Databricks Apps (point to app/05_human_approval_app.py)

8) Approve SQL candidates, then run 06_report_export_notify to export CSVs.

