import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Seus módulos customizados
from readers import get_bronze_view
from processors import run_silver_gold_logic
from utils import get_dates_to_process

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'S3_BUCKET_BRONZE', 'S3_DELTA_PATH', 'START_DATE', 'END_DATE'
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    delta_path = args['S3_DELTA_PATH'] 
    history_path = f"{delta_path.rstrip('/')}_history/" 
    gold_monthly_path = f"{delta_path.rstrip('/')}_monthly/" 

    dates = get_dates_to_process(args)

    for current_date in dates:
        date_str = current_date.strftime('%Y-%m-%d')
        
        get_bronze_view(spark, args['S3_BUCKET_BRONZE'], "purchase", current_date)
        get_bronze_view(spark, args['S3_BUCKET_BRONZE'], "product_item", current_date)
        get_bronze_view(spark, args['S3_BUCKET_BRONZE'], "extra_info", current_date)

        run_silver_gold_logic(
            spark, 
            date_str, 
            delta_path, 
            history_path, 
            gold_monthly_path
        )

    job.commit()

if __name__ == "__main__":
    main()