from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, coalesce, sum, trunc, to_date
)
from delta.tables import DeltaTable

def run_silver_gold_logic(spark, processing_date, snapshot_path, history_path, gold_monthly_path):
    only_date = processing_date[:10]
    
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW purchase_state AS
    SELECT purchase_id, 
           MAX_BY(buyer_id, transaction_datetime) AS buyer_id, 
           MAX_BY(producer_id, transaction_datetime) AS producer_id,
           MAX_BY(prod_item_id, transaction_datetime) AS prod_item_id,
           MAX_BY(order_date, transaction_datetime) AS order_date,
           MAX_BY(release_date, transaction_datetime) AS release_date,           
           MAX_BY(purchase_status, transaction_datetime) AS purchase_status,
           MAX_BY(transaction_date, transaction_datetime) AS transaction_date
    FROM purchase_eventos GROUP BY purchase_id
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW product_state AS
    SELECT purchase_id,
           MAX_BY(prod_item_id, transaction_datetime) AS prod_item_id,                
           MAX_BY(product_id, transaction_datetime) AS product_id, 
           MAX_BY(item_quantity, transaction_datetime) AS item_quantity, 
           MAX_BY(purchase_value, transaction_datetime) AS unit_price,
           MAX_BY(transaction_date, transaction_datetime) AS transaction_date
    FROM product_item_eventos GROUP BY purchase_id
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW extra_state AS
    SELECT purchase_id, 
           MAX_BY(subsidiary, transaction_datetime) AS subsidiary,
           MAX_BY(transaction_date, transaction_datetime) AS transaction_date
    FROM extra_info_eventos GROUP BY purchase_id
    """)

    history_df = None
    if DeltaTable.isDeltaTable(spark, history_path):
        history_df = DeltaTable.forPath(spark, history_path).toDF().filter(col("is_current") == True)

    def enrich_state(current_view_name, history_df, join_cols):
        current_df = spark.table(current_view_name)
        if history_df is not None:
            return (current_df.alias("curr")
                    .join(history_df.alias("hist"), "purchase_id", "outer")
                    .select(
                        col("purchase_id"),
                        *[coalesce(col(f"curr.{c}"), col(f"hist.{c}")).alias(c) for c in join_cols]
                    ))
        return current_df

    purchase_cols = ["buyer_id", "producer_id", "prod_item_id", "purchase_status", "release_date", "order_date", "transaction_date"]
    product_cols = ["product_id", "item_quantity", "unit_price", "prod_item_id", "transaction_date"]
    extra_cols = ["subsidiary", "transaction_date"]

    p_enriched = enrich_state("purchase_state", history_df, purchase_cols)
    pr_enriched = enrich_state("product_state", history_df, product_cols)
    e_enriched = enrich_state("extra_state", history_df, extra_cols)

    snapshot = (p_enriched.alias("p")
                .join(pr_enriched.alias("pr"), "purchase_id", "full")
                .join(e_enriched.alias("e"), "purchase_id", "full")
                .select(
                    to_date(lit(only_date)).alias("processing_date"),
                    col("purchase_id"),
                    col("p.buyer_id"),
                    col("p.producer_id"),
                    coalesce(col("p.prod_item_id"), col("pr.prod_item_id")).alias("prod_item_id"),
                    coalesce(col("p.transaction_date"), col("pr.transaction_date"), col("e.transaction_date")).alias("transaction_date"),
                    col("pr.product_id"),
                    col("e.subsidiary"),
                    col("p.purchase_status"),
                    col("p.release_date"),
                    col("p.order_date"),
                    col("pr.item_quantity"),
                    col("pr.unit_price"),
                    (coalesce(col("pr.item_quantity"), lit(0)) * coalesce(col("pr.unit_price"), lit(0))).alias("item_gmv")
                ))

    snapshot_to_save = snapshot.filter(col("transaction_date") == lit(only_date))
    
    final_count = snapshot_to_save.count()
    
    if final_count > 0:
        (snapshot_to_save.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"transaction_date = '{only_date}'")
            .save(snapshot_path))

    snapshot_scd = snapshot.withColumn(
        "hash_diff",
        sha2(concat_ws("||", 
            coalesce(col("prod_item_id"), lit("")),
            coalesce(col("order_date").cast("string"), lit("")),
            coalesce(col("release_date").cast("string"), lit("")),
            coalesce(col("purchase_status"), lit("")),            
            coalesce(col("buyer_id"), lit("")),
            coalesce(col("producer_id"), lit("")),
            coalesce(col("product_id"), lit("")),
            coalesce(col("subsidiary"), lit("")),
            coalesce(col("item_quantity").cast("string"), lit("")),
            coalesce(col("unit_price").cast("string"), lit(""))
        ), 256)
    )

    if not DeltaTable.isDeltaTable(spark, history_path):
        (snapshot_scd
            .withColumn("valid_from", col("transaction_date"))
            .withColumn("valid_to", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
            .withColumn("created_at", current_timestamp())
            .write.format("delta").mode("overwrite").save(history_path))
    else:
        history_table = DeltaTable.forPath(spark, history_path)
        
        # Identifica quem mudou para fechar a versão antiga
        updates_df = snapshot_scd.alias("src").join(
            history_table.toDF().alias("tgt"),
            (col("src.purchase_id") == col("tgt.purchase_id")) & (col("tgt.is_current") == True)
        ).filter("src.hash_diff <> tgt.hash_diff")

        staging_df = snapshot_scd.withColumn("merge_key", col("purchase_id")) \
            .unionByName(updates_df.select("src.*").withColumn("merge_key", lit(None)))

        history_table.alias("tgt").merge(
            staging_df.alias("src"),
            "tgt.purchase_id = src.merge_key AND tgt.is_current = true"
        ).whenMatchedUpdate(
            condition="tgt.hash_diff <> src.hash_diff",
            set={"valid_to": "src.transaction_date", "is_current": "false"}
        ).whenNotMatchedInsert(
            values={
                "transaction_date": "src.transaction_date",
                "purchase_id": "src.purchase_id", "buyer_id": "src.buyer_id",
                "prod_item_id": "src.prod_item_id", "release_date": "src.release_date",
                "order_date": "src.order_date", "purchase_status": "src.purchase_status",
                "producer_id": "src.producer_id", "product_id": "src.product_id",
                "subsidiary": "src.subsidiary", "item_quantity": "src.item_quantity",
                "unit_price": "src.unit_price", "item_gmv": "src.item_gmv",
                "hash_diff": "src.hash_diff", "valid_from": "src.transaction_date",
                "valid_to": "NULL", "is_current": "True", "created_at": "current_timestamp()"
            }
        ).execute()

    monthly_gmv = (
        snapshot
        .withColumn("reference_month", trunc(col("transaction_date"), "month"))
        .groupBy("reference_month", "subsidiary")
        .agg(sum("item_gmv").alias("monthly_gmv"))
        .withColumn("as_of_date", to_date(lit(only_date)))
        .withColumn("created_at", current_timestamp())
    )

    (monthly_gmv.write.format("delta")
        .mode("overwrite")
        .partitionBy("as_of_date") # Permite navegar entre fechamentos de dias diferentes
        .option("replaceWhere", f"as_of_date = '{only_date}'")
        .save(gold_monthly_path))