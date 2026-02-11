CREATE OR REPLACE TEMP VIEW purchase_extra_info_state AS WITH pe_rn AS
  (SELECT purchase_id,
          purchase_partition,
          subsidiary,
          ROW_NUMBER() OVER (PARTITION purchase_id, purchase_partition
                             ORDER BY transaction_datetime DESC) AS rn
   FROM purchase_extra_info
   WHERE transaction_date < CURRENT_DATE())
SELECT *
FROM pe_rn
WHERE rn = 1;