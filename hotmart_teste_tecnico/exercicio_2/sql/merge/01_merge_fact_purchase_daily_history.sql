MERGE INTO fact_purchase_daily_history AS tgt
USING snapshot_d1 AS src
ON  tgt.purchase_id = src.purchase_id
AND tgt.product_item_id = src.product_item_id
AND tgt.record_end_date = DATE '9999-12-31'

WHEN MATCHED
 AND tgt.event_hash <> src.event_hash
THEN UPDATE SET
  tgt.record_end_date = src.transaction_date - 1,
  tgt.is_active = FALSE

WHEN NOT MATCHED
THEN INSERT (
  transaction_date,
  purchase_id,
  product_item_id,
  product_id,
  quantity,
  unit_price,
  item_gmv,
  purchase_status,
  currency,
  is_active,
  record_start_date,
  record_end_date,
  ingestion_date,
  event_hash
)
VALUES (
  src.transaction_date,
  src.purchase_id,
  src.product_item_id,
  src.product_id,
  src.quantity,
  src.unit_price,
  src.item_gmv,
  src.purchase_status,
  src.currency,
  TRUE,
  src.record_start_date,
  src.record_end_date,
  src.ingestion_date,
  src.event_hash
);