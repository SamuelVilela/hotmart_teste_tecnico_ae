CREATE OR REPLACE TEMP VIEW snapshot_d1 AS
SELECT
  CURRENT_DATE() - 1 AS transaction_date,

  coalesce(p.purchase_id, pe.purchase_id) as purchase_id
  pi.prod_item_id,
  pi.product_id,

  p.release_date,
  p.order_date,

  p.buyer_id,
  p.producer_id,
  pe.subsidiary,

  pi.item_quantity,
  pi.unit_price,
  pi.item_quantity * pi.unit_price AS item_gmv,

  p.purchase_status,



  TRUE AS is_active,
  CURRENT_DATE() - 1 AS record_start_date,
  DATE '9999-12-31' AS record_end_date,
  CURRENT_DATE() AS ingestion_date,

  SHA1(
    CONCAT_WS('|',
      p.purchase_id,
      pi.prod_item_id,     
      P.transaction_date
    )
  ) AS event_hash

FROM purchase_state p
LEFT JOIN product_item_state pi
  ON p.prod_item_id = pi.prod_item_id
  and p.prod_item_partition = pi.prod_item_partition
full JOIN purchase_extra_info_state pe
  ON p.purchase_id = pe.purchase_id
 AND p.purchase_partition = pe.purchase_partition;