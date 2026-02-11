CREATE OR REPLACE TEMP VIEW product_item_state AS WITH pi_rn AS
  (SELECT prod_item_id,
          prod_item_partition,
          product_id,
          item_quantity,
          purchase_value,
          ROW_NUMBER() OVER (PARTITION prod_item_id, prod_item_partition
                             ORDER BY transaction_datetime DESC) AS rn
   FROM product_item
   WHERE transaction_date < CURRENT_DATE())
SELECT *
FROM pi_rn
WHERE rn =1;