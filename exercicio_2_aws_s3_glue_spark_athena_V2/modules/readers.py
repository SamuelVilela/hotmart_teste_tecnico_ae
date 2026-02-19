from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import col, lit  # <--- ESSENCIAL: Faltava importar o col
from datetime import datetime
SCHEMAS = {
    "purchase": StructType([
        StructField("purchase_id", StringType(), True),
        StructField("buyer_id", StringType(), True),
        StructField("producer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("prod_item_id", StringType(), True),
        StructField("release_date", TimestampType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("transaction_datetime", TimestampType(), True),
        StructField("purchase_status", StringType(), True),
        StructField("transaction_date", StringType(), True)
    ]),
    "product_item": StructType([
        StructField("purchase_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("prod_item_id", StringType(), True),
        StructField("item_quantity", IntegerType(), True),
        StructField("purchase_value", DoubleType(), True),
        StructField("transaction_datetime", TimestampType(), True),
        StructField("transaction_date", StringType(), True)
    ]),
    "extra_info": StructType([
        StructField("purchase_id", StringType(), True),
        StructField("subsidiary", StringType(), True),
        StructField("transaction_datetime", TimestampType(), True),
        StructField("transaction_date", StringType(), True)
    ])
}

def get_bronze_view(spark, bucket, table_name, current_date):
    if isinstance(current_date, str):
        current_date = datetime.strptime(current_date[:10], '%Y-%m-%d')
    
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    
    path = f"s3://{bucket}/{table_name}/year={year}/month={month}/day={day}/"
    print(f"### [DEBUG] Acessando Path: {path}")
    try:
        df_raw = spark.read.parquet(path)
        schema_fixo = SCHEMAS.get(table_name)
        
        select_expr = []
        for field in schema_fixo.fields:
            if field.name in df_raw.columns:
                select_expr.append(col(field.name).cast(field.dataType))
            else:
                select_expr.append(lit(None).cast(field.dataType).alias(field.name))
        
        df_final = df_raw.select(*select_expr)
        df_final.createOrReplaceTempView(f"{table_name}_eventos")
        
       
    except Exception as e:
        schema_fixo = SCHEMAS.get(table_name)
        empty_df = spark.createDataFrame([], schema_fixo)
        empty_df.createOrReplaceTempView(f"{table_name}_eventos")

    return True