CREATE TABLE IF NOT EXISTS fact_purchase_daily_history (
  transaction_date      DATE,
  purchase_id           STRING,
  product_item_id       STRING,
  product_id            STRING,
  quantity              INT,
  unit_price            DECIMAL(18,2),
  item_gmv              DECIMAL(18,2),
  purchase_status       STRING,
  currency              STRING,
  is_active             BOOLEAN,
  record_start_date     DATE,
  record_end_date       DATE,
  ingestion_date        DATE,
  event_hash            STRING
)
USING DELTA
PARTITIONED BY (transaction_date);
