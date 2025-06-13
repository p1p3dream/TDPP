import sys
import logging
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, date_format, concat, coalesce, col, sha2, to_date
from awsglue.dynamicframe import DynamicFrame

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    logger.info("Creating Spark session")
    return (SparkSession.builder
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
            .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
            .config('spark.sql.hive.convertMetastoreParquet', 'false')
            .config('spark.sql.adaptive.enabled', 'true')
            .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            .config('spark.sql.adaptive.skewJoin.enabled', 'true')
            .config('spark.dynamicAllocation.enabled', 'true')
            .config('spark.sql.files.maxPartitionBytes', '128m')
            .config('spark.default.parallelism', '200')
            .getOrCreate())

def init_job(spark):
    logger.info("Initializing Glue job")
    sc = spark.sparkContext
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    return glue_context, job

def get_job_args():
    logger.info("Getting job arguments")
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'start_date', 'end_date'])

def create_query(start_date, end_date):
    logger.info(f"Creating query for date range: {start_date} to {end_date}")
    return f"""
    SELECT *
    , 'bigquery' as source_system
    , 'ga4' as original_source
    , 'analytics_408060011' as schema
    FROM `ga4-to-big-query-391018.analytics_408060011.events_*`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE('{start_date}')) 
                            AND FORMAT_DATE('%Y%m%d', DATE('{end_date}'))
    """

def read_dynamic_frame(glue_context, query):
    logger.info("Reading data from BigQuery")
    return glue_context.create_dynamic_frame.from_options(
        connection_type="bigquery",
        connection_options={
            "executed_by": "$service_principal",
            "parentProject": "ga4-to-big-query-391018",
            "query": query,
            "connectionName": "Bigquery connection",
            "materializationDataset": "analytics_408060011",
            "viewsEnabled": "true"
        }
    )

def add_partition_columns(df, partition_field):
    logger.info("Adding partition columns")
    return (df.withColumn("ingestion_dt", to_date(lit(partition_field)))
              .withColumn("year", date_format(col('ingestion_dt'), "yyyy"))
              .withColumn("month", date_format(col('ingestion_dt'), "MM"))
              .withColumn("day", date_format(col('ingestion_dt'), "dd")))

def get_hudi_options():
    logger.info("Getting Hudi options")
    return {
        'hoodie.table.name': 'tdp_bronze_ga4_bigquery_events_analytics_408060011', 
        'hoodie.datasource.write.table.type': "COPY_ON_WRITE",
        'hoodie.datasource.write.operation': "bulk_insert", 
        'hoodie.datasource.write.recordkey.field': "event_timestamp,event_name,user_pseudo_id",
        "hoodie.datasource.write.precombine.field": "ingestion_dt",
        'hoodie.datasource.write.partitionpath.field': 'year,month,day',
        'hoodie.datasource.write.hive_style_partitioning': "true", 
        'hoodie.parquet.compression.codec': "snappy",
        'hoodie.bulkinsert.shuffle.parallelism': 200,
        'hoodie.insert.shuffle.parallelism': 200,
    }

def write_to_hudi(df, output_path, hudi_options):
    logger.info(f"Writing data to Hudi: {output_path}")
    df.write.format('hudi').options(**hudi_options).mode("append").save(output_path)

def process_batch(glue_context, start_date, end_date, output_path, hudi_options, partition_field):
    logger.info(f"Processing batch for date range: {start_date} to {end_date}")
    query = create_query(start_date, end_date)
    dynamic_frame_read = read_dynamic_frame(glue_context, query)
    df = dynamic_frame_read.toDF()
    logger.info(f"Read {df.count()} rows from BigQuery")
    df_with_partitions = add_partition_columns(df, partition_field)
    logger.info(f"Writing to hudi...")
    write_to_hudi(df_with_partitions, f'{output_path}', hudi_options)
    logger.info("Batch processing completed")

def main():
    partition_field = datetime.utcnow().strftime('%Y-%m-%d') # in bronze layer we use the ingestion date for partitions
    
    logger.info("Starting Glue job")
    # setup
    spark = create_spark_session()
    glue_context, job = init_job(spark)
    args = get_job_args()
    # kick job
    job.init(args['JOB_NAME'], args)
    
    # Get start and end dates from job arguments
    start_date = args['start_date']
    end_date = args['end_date']
    
    # hudi setup
    hudi_options = get_hudi_options()
    output_path = 's3://cloudformation-glue-test/bronze/ga4-bigquery/events/analytics_408060011/'
    
    # execution
    process_batch(glue_context, start_date, end_date, output_path, hudi_options, partition_field)
    # always commit
    job.commit()
    logger.info("Glue job completed successfully")

if __name__ == "__main__":
    main()