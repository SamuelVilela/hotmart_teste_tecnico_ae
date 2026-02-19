CREATE EXTERNAL TABLE IF NOT EXISTS gold_gmv_history (
  reference_month DATE,
  purchase_id STRING,
  transaction_date DATE,
  subsidiary STRING,
  purchase_status STRING,
  buyer_id STRING,
  item_quantity INT,
  unit_price DOUBLE,
  item_gmv DOUBLE,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN,
  created_at TIMESTAMP
)
PARTITIONED BY (transaction_date STRING) 
LOCATION 's3://bucket-gold-hotmart-2/gold_gmv_history/'
TBLPROPERTIES ('table_type'='DELTA');

SQL
CREATE OR REPLACE VIEW view_gmv_history_analysis AS
SELECT 
    row_number() OVER (
        PARTITION BY purchase_id, transaction_date 
        ORDER BY created_at DESC
    ) AS line,
    date_trunc('month', CAST(transaction_date AS DATE)) AS reference_month, 
    purchase_id, 
    transaction_date, 
    subsidiary,
    purchase_status,
    buyer_id,
    item_quantity,
    unit_price,
    item_gmv,
    valid_from,
    valid_to,
    is_current,
    created_at
FROM gold_gmv_history;
