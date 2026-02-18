from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    concat_ws,
    coalesce,
    sum,
    trunc
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import sys
import os

# =========================================================
# Spark Session
# =========================================================

def create_spark():
    builder = (
        SparkSession.builder
        .appName("Lakehouse-Pipeline")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=./spark-temp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


# =========================================================
# Date Handling (D-1 + Backfill)
# =========================================================

def get_execution_dates():
    args = sys.argv[1:]

    if len(args) == 0:
        return [(datetime.today() - timedelta(days=1)).date()]

    if len(args) == 1:
        return [datetime.strptime(args[0], "%Y-%m-%d").date()]

    if len(args) == 2:
        start = datetime.strptime(args[0], "%Y-%m-%d")
        end = datetime.strptime(args[1], "%Y-%m-%d")

        if end < start:
            raise ValueError("End date cannot be earlier than start date")

        return [
            (start + timedelta(days=i)).date()
            for i in range((end - start).days + 1)
        ]

    raise ValueError("Usage: python main.py [date] or start_date end_date")


# =========================================================
# Bronze Layer (Simulated Events)
# =========================================================

def load_bronze(spark):

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
    ('2023-07-12 09:00:00','2023-07-12',55,696969,10,55.00),
    ('2023-01-25 23:59:59','2023-01-25',56,808080,120,2400.00),
    ('2023-02-26 03:00:00','2023-02-26',69,373737,2,2000.00)
    AS t(transaction_datetime, transaction_date, purchase_id, product_id, item_quantity, purchase_value)
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW purchase_extra_info_eventos AS
    SELECT * FROM VALUES
    ('2023-01-23 00:05:00','2023-01-23',55,'nacional'),
    ('2023-03-12 07:00:00','2023-03-12',55,'internacional'),
    ('2023-01-25 23:59:59','2023-01-25',56,'internacional'),
    ('2023-02-28 01:10:00','2023-02-28',69,'nacional')
    AS t(transaction_datetime, transaction_date, purchase_id, subsidiary)
    """)


# =========================================================
# Execution per Date
# =========================================================

def run_for_date(spark, processing_date, snapshot_path, history_path, gold_monthly_path):

    print(f"Processing date: {processing_date}")

    # -----------------------------
    # Silver - Reconstruct State
    # -----------------------------

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW purchase_clean AS
    SELECT *
    FROM purchase_eventos
    WHERE transaction_date <= DATE('{processing_date}')
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW product_clean AS
    SELECT *
    FROM product_item_eventos
    WHERE transaction_date <= DATE('{processing_date}')
    """)

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW extra_clean AS
    SELECT *
    FROM purchase_extra_info_eventos
    WHERE transaction_date <= DATE('{processing_date}')
    """)

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

    # -----------------------------
    # Snapshot Table
    # -----------------------------

    if not DeltaTable.isDeltaTable(spark, snapshot_path):

        snapshot.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("transaction_date") \
            .save(snapshot_path)

    else:

        snapshot.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"transaction_date = '{processing_date}'") \
            .save(snapshot_path)

    # -----------------------------
    # SCD2 History
    # -----------------------------

    snapshot_scd = (
        snapshot
        .withColumn(
            "hash_diff",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("product_id").cast("string"), lit("")),
                    coalesce(col("subsidiary"), lit("")),
                    coalesce(col("item_quantity").cast("string"), lit("")),
                    coalesce(col("unit_price").cast("string"), lit(""))
                ),
                256
            )
        )
        .withColumn("valid_from", col("transaction_date"))
        .withColumn("valid_to", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("created_at", current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, history_path):
        snapshot_scd.write.format("delta").mode("overwrite").save(history_path)

    history_table = DeltaTable.forPath(spark, history_path)

    history_table.alias("tgt").merge(
        snapshot_scd.alias("src"),
        "tgt.purchase_id = src.purchase_id AND tgt.is_current = true"
    ).whenMatchedUpdate(
        condition="tgt.hash_diff <> src.hash_diff",
        set={
            "valid_to": "src.valid_from",
            "is_current": "false"
        }
    ).whenNotMatchedInsertAll().execute()

    # -----------------------------
    # GOLD - Monthly GMV Versioned
    # -----------------------------

    snapshot_df = spark.read.format("delta").load(snapshot_path)

    monthly_gmv = (
        snapshot_df
        .withColumn("reference_month", trunc(col("transaction_date"), "month"))
        .groupBy("reference_month", "subsidiary")
        .agg(sum("item_gmv").alias("monthly_gmv"))
        .withColumn("as_of_date", lit(processing_date))
        .withColumn("created_at", current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, gold_monthly_path):

        monthly_gmv.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("as_of_date") \
            .save(gold_monthly_path)

    else:

        monthly_gmv.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"as_of_date = '{processing_date}'") \
            .save(gold_monthly_path)

    print(f"Finished processing for {processing_date}")


# =========================================================
# MAIN
# =========================================================

def main():

    spark = create_spark()

    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.sparkContext.setLogLevel("ERROR")

    base_path = "./lakehouse"
    snapshot_path = f"{base_path}/fact_purchase_snapshot"
    history_path = f"{base_path}/fact_purchase_history_scd2"
    gold_monthly_path = f"{base_path}/gold_gmv_monthly_snapshot"

    os.makedirs(base_path, exist_ok=True)

    execution_dates = get_execution_dates()
    load_bronze(spark)

    for date in execution_dates:
        run_for_date(
            spark,
            str(date),
            snapshot_path,
            history_path,
            gold_monthly_path
        )

    spark.catalog.clearCache()
    spark.stop()


if __name__ == "__main__":
    main()
