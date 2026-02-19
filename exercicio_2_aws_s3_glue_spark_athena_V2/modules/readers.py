from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import col, lit  # <--- ESSENCIAL: Faltava importar o col
from datetime import datetime
# Dicionário de esquemas permanece igual (está correto usando StringType nos IDs)
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
        # O [:10] garante que '2026-02-18T12:40...' vire apenas '2026-02-18'
        current_date = datetime.strptime(current_date[:10], '%Y-%m-%d')
    
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    
    path = f"s3://{bucket}/{table_name}/year={year}/month={month}/day={day}/"
    print(f"### [DEBUG] Acessando Path: {path}")
    try:
        # 1. Lê os dados deixando o Spark detectar o tipo original (evita o erro de conversão)
        df_raw = spark.read.parquet(path)
        
        # 2. Aplica o esquema do dicionário usando SELECT e CAST
        # Isso é mais seguro que o .schema() direto para Parquet
        schema_fixo = SCHEMAS.get(table_name)
        
        # Criamos as expressões de cast dinamicamente com base no seu dicionário
        select_expr = []
        for field in schema_fixo.fields:
            if field.name in df_raw.columns:
                select_expr.append(col(field.name).cast(field.dataType))
            else:
                # Se a coluna não existe no arquivo, cria como nula com o tipo correto
                select_expr.append(lit(None).cast(field.dataType).alias(field.name))
        
        df_final = df_raw.select(*select_expr)
        df_final.createOrReplaceTempView(f"{table_name}_eventos")
        
        print(f"### [SUCCESS] {table_name} carregada e convertida.")
        
    except Exception as e:
        print(f"### [EMPTY] {table_name} não encontrada. Criando view vazia.")
        schema_fixo = SCHEMAS.get(table_name)
        empty_df = spark.createDataFrame([], schema_fixo)
        empty_df.createOrReplaceTempView(f"{table_name}_eventos")

        print(f"### [ERROR] Falha ao carregar {table_name}: {str(e)}") # Isso vai te dizer o erro real
        print(f"### [EMPTY] {table_name} não encontrada. Criando view vazia.")
        # ... resto do código de view vazia ..
    
    return True