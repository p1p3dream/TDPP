import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
    
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1717769767934 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://tdp-bronze/tdp_all_sales/"], "recurse": True, "mergeSchema": True}, transformation_ctx="AmazonS3_node1717769767934")

#Convert S3 output to dataframe, filter for most recent data pulled
df = AmazonS3_node1717769767934.toDF()
max_ts = df.agg({'glue_pull_datetime_utc': 'max'}).collect()[0][0]
# df_filtered = AmazonS3_node1717769767934.filter(f=lambda x: x['glue_pull_datetime_utc'] == max_ts)
df_filtered = df.filter(df['glue_pull_datetime_utc'] == max_ts)
print("Max Timestamp pulled from s3:", max_ts)
print("Total S3 records:", df.count())
print("Latest records:", df_filtered.count())
# df_filtered.toDF().show(5)

#Convert dataframe back to dynamic frame for redshift write
dfc = DynamicFrame.fromDF(df_filtered, glueContext, "dfc")
print("DFC number of records:", dfc.count())

# Script generated for node Amazon Redshift
AmazonRedshift_node1717769791881 = glueContext.write_dynamic_frame.from_options(
    frame=dfc, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-076579646618-us-east-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "gold.tdp_all_sales", 
        "connectionName": "tdp redshift connection", 
        "preactions": """
        CREATE TABLE IF NOT EXISTS
            gold.tdp_all_sales (
                data_type VARCHAR, 
                source_system VARCHAR, 
                source_system_id VARCHAR,
                unified_store_id INTEGER, 
                order_id VARCHAR, 
                order_item_id VARCHAR, 
                order_created_datetime TIMESTAMP, 
                order_completed_datetime TIMESTAMP, 
                order_created_datetime_utc TIMESTAMP, 
                order_completed_datetime_utc TIMESTAMP, 
                accounting_date DATE, 
                order_created_by VARCHAR, 
                order_completed_by VARCHAR, 
                order_fulfillment_method VARCHAR, 
                mrw_flag VARCHAR,
                ecomm_order_id varchar,
                customer_id VARCHAR, 
                unified_customer_id BIGINT, 
                product_id VARCHAR, 
                unified_product_id BIGINT, 
                batch VARCHAR, 
                order_type VARCHAR, 
                invoice_type VARCHAR, 
                gross_sales DOUBLE PRECISION, 
                discount_amount DOUBLE PRECISION, 
                net_sales DOUBLE PRECISION, 
                quantity DOUBLE PRECISION, 
                cogs DOUBLE PRECISION, 
                cost DOUBLE PRECISION,
                pull_datetime TIMESTAMP, 
                pull_datetime_utc TIMESTAMP, 
                glue_pull_datetime_utc TIMESTAMP
            );
        DELETE FROM gold.tdp_all_sales WHERE ACCOUNTING_DATE BETWEEN ADD_MONTHS(DATEADD(day, 1, LAST_DAY(CURRENT_DATE)), -2) AND DATEADD(day, -1, CURRENT_DATE);
        """
    }, 
    transformation_ctx="AmazonRedshift_node1717769791881"
)

job.commit()