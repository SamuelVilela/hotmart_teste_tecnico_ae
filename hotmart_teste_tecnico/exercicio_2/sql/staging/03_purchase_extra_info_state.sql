CREATE OR REPLACE TEMP VIEW purchase_extra_state AS
SELECT
  purchase_id,
  purchase_partition,
  MAX_BY(subsidiary, transaction_datetime) AS subsidiary
FROM purchase_extra_info
WHERE transaction_date < CURRENT_DATE()
GROUP BY purchase_id, purchase_partition;