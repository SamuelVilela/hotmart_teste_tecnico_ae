SELECT transaction_date, 
subsidiary, 
SUM(item_gmv) AS daily_gmv 
FROM view_gmv_history_analysis
 WHERE is_current = true 
 and purchase_status ='APROVADA' 
 GROUP BY 1, 2 
 ORDER BY 1 DESC;