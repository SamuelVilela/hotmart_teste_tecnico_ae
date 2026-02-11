CREATE OR REPLACE TEMP VIEW purchase_state AS


SELECT
  purchase_id,
  purchase_partition,
  buyer_id AS buyer_id,
  producer_id AS producer_id,
  purchase_status AS purchase_status,
  purchase_total_value AS purchase_total_value,
  prod_item_id AS prod_item_id,
  order_date AS order_date,
  release_date AS release_date,
  prod_item_partition AS prod_item_partition,
  transaction_date
  
FROM purchase
WHERE transaction_date < CURRENT_DATE()
qualify ROW_NUMBER() OVER (PARTITION purchase_id, purchase_partition ORDER by transaction_datetime desc) = 1
GROUP BY purchase_id, purchase_partition;