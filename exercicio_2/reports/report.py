from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

def create_spark():
    builder = (
        SparkSession.builder
        .appName("Lakehouse-Report")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_report():

    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    base_path = "./teste_hotmart/exercicio_2/pipeline_gmv/lakehouse"
    history_path = f"{base_path}/fact_purchase_history_scd2"
    gold_path = f"{base_path}/gold_gmv_monthly_snapshot"

    print("\n===== HISTÓRICO SCD2 =====")
    try:
        history_df = spark.read.format("delta").load(history_path)
        history_df.show(truncate=False)

        history_df.groupBy("is_current").count().show()
    except:
        print("Histórico ainda não gerado.")

    print("\n===== GOLD GMV MENSAL =====")
    try:
        gold_df = spark.read.format("delta").load(gold_path)
        gold_df.show(truncate=False)

        gold_df.groupBy("subsidiary").agg(sum("monthly_gmv")).show()
    except:
        print("Gold ainda não gerado.")

    spark.stop()

if __name__ == "__main__":
    run_report()
