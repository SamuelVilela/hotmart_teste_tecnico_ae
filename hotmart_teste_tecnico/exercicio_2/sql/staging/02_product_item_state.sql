CREATE OR REPLACE TEMP VIEW product_item_state AS
SELECT
  prod_item_id,
  prod_item_partition,
  MAX_BY(product_id, transaction_datetime) AS product_id,
  MAX_BY(item_quantity, transaction_datetime) AS item_quantity,
  MAX_BY(purchase_value, transaction_datetime) AS unit_price
FROM product_item
WHERE transaction_date < CURRENT_DATE()
GROUP BY prod_item_id, prod_item_partition;