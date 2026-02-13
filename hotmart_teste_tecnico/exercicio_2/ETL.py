from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import expr
import os

# -------------------------------------------------
# 1️⃣ Spark Config
# -------------------------------------------------

builder = SparkSession.builder \
    .appName("Purchase-ETL-Final") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

processing_date = "2023-07-15"
base_path = "./lakehouse"
snapshot_path = f"{base_path}/fact_purchase_snapshot"
history_path = f"{base_path}/fact_purchase_history_scd2"

os.makedirs(base_path, exist_ok=True)

# -------------------------------------------------
# 2️⃣ Bronze (dados simulados)
# -------------------------------------------------

spark.sql("""
CREATE OR REPLACE TEMP VIEW purchase_eventos AS
SELECT * FROM VALUES
('2023-01-20 22:00:00','2023-01-20',55,15947,852852),
('2023-02-05 10:00:00','2023-02-05',55,160001,852852),
('2023-01-26 00:01:00','2023-01-26',56,36798,963963),
('2023-02-26 03:00:00','2023-02-26',69,160001,96967)
AS t(transaction_datetime, transaction_date, purchase_id, buyer_id, producer_id)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW product_item_eventos AS
SELECT * FROM VALUES
('2023-01-20 22:02:00','2023-01-20',55,696969,10,50.00),
('2023-01-24 22:02:00','2023-01-24',55,696969,10,60.00),
('2023-07-12 09:00:00','2023-07-12',55,696969,10,55.00), -- alteração preço
('2023-01-25 23:59:59','2023-01-25',56,808080,120,2400.00),
('2023-02-26 03:00:00','2023-02-26',69,373737,2,2000.00)
AS t(transaction_datetime, transaction_date, purchase_id, product_id, item_quantity, purchase_value)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW purchase_extra_info_eventos AS
SELECT * FROM VALUES
('2023-01-23 00:05:00','2023-01-23',55,'nacional'),
('2023-03-12 07:00:00','2023-03-12',55,'internacional'), -- alteração subsidiária
('2023-01-25 23:59:59','2023-01-25',56,'internacional'),
('2023-02-28 01:10:00','2023-02-28',69,'nacional')
AS t(transaction_datetime, transaction_date, purchase_id, subsidiary)
""")

# -------------------------------------------------
# 3️⃣ Qualidade dos Dados
# -------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW purchase_clean AS
SELECT *
FROM purchase_eventos
WHERE purchase_id IS NOT NULL
AND buyer_id IS NOT NULL
AND transaction_date <= DATE('{processing_date}')
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW product_clean AS
SELECT *
FROM product_item_eventos
WHERE item_quantity > 0
AND purchase_value >= 0
AND transaction_date <= DATE('{processing_date}')
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW extra_clean AS
SELECT *
FROM purchase_extra_info_eventos
WHERE subsidiary IS NOT NULL
AND transaction_date <= DATE('{processing_date}')
""")

# -------------------------------------------------
# 4️⃣ Silver - Estado Atual
# -------------------------------------------------

spark.sql("""
CREATE OR REPLACE TEMP VIEW purchase_state AS
SELECT purchase_id,
       MAX_BY(buyer_id, transaction_datetime) AS buyer_id,
       MAX_BY(producer_id, transaction_datetime) AS producer_id
FROM purchase_clean
GROUP BY purchase_id
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW product_state AS
SELECT purchase_id,
       MAX_BY(product_id, transaction_datetime) AS product_id,
       MAX_BY(item_quantity, transaction_datetime) AS item_quantity,
       MAX_BY(purchase_value, transaction_datetime) AS unit_price
FROM product_clean
GROUP BY purchase_id
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW extra_state AS
SELECT purchase_id,
       MAX_BY(subsidiary, transaction_datetime) AS subsidiary
FROM extra_clean
GROUP BY purchase_id
""")


# -------------------------------------------------
# 5️⃣ Snapshot Diário (Gold)
# -------------------------------------------------

snapshot = spark.sql(f"""
SELECT
  DATE('{processing_date}') AS transaction_date,
  p.purchase_id,
  p.buyer_id,
  p.producer_id,
  pr.product_id,
  e.subsidiary,
  pr.item_quantity,
  pr.unit_price,
  pr.item_quantity * pr.unit_price AS item_gmv
FROM purchase_state p
LEFT JOIN product_state pr USING (purchase_id)
LEFT JOIN extra_state e USING (purchase_id)
""")

snapshot.write.format("delta").mode("overwrite").save(snapshot_path)

print("\n🔥 Snapshot Atual")
spark.read.format("delta").load(snapshot_path).show(truncate=False)

# -------------------------------------------------
# 6️⃣ Histórico SCD2 - 100% Idempotente
# -------------------------------------------------

from pyspark.sql.functions import sha2, concat_ws, coalesce

# 1️⃣ Criar hash
snapshot_scd = snapshot \
    .withColumn("hash_diff",
        sha2(concat_ws("||",
            coalesce(col("product_id").cast("string"), lit("")),
            coalesce(col("subsidiary"), lit("")),
            coalesce(col("item_quantity").cast("string"), lit("")),
            coalesce(col("unit_price").cast("string"), lit(""))
        ), 256)
    ) \
    .withColumn("valid_from", col("transaction_date")) \
    .withColumn("valid_to", lit(None).cast("date")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("created_at", current_timestamp())

# Criar tabela se não existir
if not DeltaTable.isDeltaTable(spark, history_path):
    snapshot_scd.write.format("delta").mode("overwrite").save(history_path)

history_table = DeltaTable.forPath(spark, history_path)
history_df = spark.read.format("delta").load(history_path)

# 2️⃣ Filtrar apenas registros atuais
current_df = history_df.filter(col("is_current") == True)

# 3️⃣ Detectar novos ou alterados
changed_df = snapshot_scd.alias("src").join(
    current_df.alias("tgt"),
    "purchase_id",
    "left"
).where(
    (col("tgt.purchase_id").isNull()) |
    (col("tgt.hash_diff") != col("src.hash_diff"))
).select("src.*")

# 4️⃣ Fechar versões atuais que mudaram
history_table.alias("tgt").merge(
    changed_df.alias("src"),
    "tgt.purchase_id = src.purchase_id AND tgt.is_current = true"
).whenMatchedUpdate(
    set={
        "valid_to": "src.valid_from",
        "is_current": "false"
    }
).execute()

# 5️⃣ Inserir nova versão somente se ainda não existir
history_table.alias("tgt").merge(
    changed_df.alias("src"),
    """
    tgt.purchase_id = src.purchase_id
    AND tgt.valid_from = src.valid_from
    """
).whenNotMatchedInsertAll() \
.execute()

print("\n📜 Histórico SCD2")
spark.read.format("delta").load(history_path) \
    .orderBy("purchase_id", "valid_from") \
    .show(truncate=False)

# -------------------------------------------------
# 7️⃣ GMV Diário por Subsidiária
# -------------------------------------------------

print("\n💰 GMV Diário por Subsidiária")

spark.read.format("delta").load(snapshot_path) \
    .groupBy("transaction_date", "subsidiary") \
    .agg(sum("item_gmv").alias("daily_gmv")) \
    .orderBy("transaction_date") \
    .show(truncate=False)

spark.stop()

