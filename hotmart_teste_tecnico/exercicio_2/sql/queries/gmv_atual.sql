SELECT
  SUM(item_gmv) AS gmv_atual
FROM fact_purchase_daily_history
WHERE transaction_date = CURRENT_DATE() - 1
  AND is_active = TRUE
  AND purchase_status = 'PAID';