SELECT
  SUM(item_gmv) AS gmv_jan_2023
FROM fact_purchase_daily_history
WHERE transaction_date BETWEEN DATE '2023-01-01' AND DATE '2023-01-31'
  AND is_active = TRUE;