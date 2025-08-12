SELECT accountType, AVG(amount) AS avg_amount, SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count
FROM finance.kyc_gold.customer_enriched
GROUP BY accountType
ORDER BY fraud_count DESC;