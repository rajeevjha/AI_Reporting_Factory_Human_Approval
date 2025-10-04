%sql

DELETE FROM finance.kyc_bronze.paysim_raw;

DELETE FROM finance.kyc_silver.transactions_clean;

DELETE FROM finance.kyc_gold.customer_enriched;

DELETE FROM finance.kyc_gold.ai_sql_candidates;

DELETE FROM finance.kyc_gold.report_candidates;



