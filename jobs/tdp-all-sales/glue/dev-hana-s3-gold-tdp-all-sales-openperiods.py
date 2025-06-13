import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'environment'])
bucket_prefix = "tdp-bronze-prod" if args['environment'] == "prod" else "tdp-bronze"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node SAP HANA
SAPHANA_node1717771614827 = glueContext.create_dynamic_frame.from_options(connection_type="saphana", connection_options={"connectionName": "Saphana connection", "query": "SELECT\r\n    DATA_TYPE,\r\n    SOURCE_SYSTEM,\r\n    SOURCE_SYSTEM_ID,\r\n    UNIFIED_STORE_ID,\r\n    ORDER_ID,\r\n    ORDER_ITEM_ID,\r\n    ORDER_CREATED_DATETIME,\r\n    ORDER_COMPLETED_DATETIME,\r\n    ORDER_CREATED_DATETIME_UTC,\r\n    ORDER_COMPLETED_DATETIME_UTC,\r\n    ACCOUNTING_DATE,\r\n    ORDER_CREATED_BY,\r\n    ORDER_COMPLETED_BY,\r\n    ORDER_FULFILLMENT_METHOD,\r\n    MRW_FLAG,\r\n    ECOMM_ORDER_ID,\r\n    CUSTOMER_ID,\r\n    UNIFIED_CUSTOMER_ID,\r\n    PRODUCT_ID,\r\n    UNIFIED_PRODUCT_ID,\r\n    BATCH,\r\n    ORDER_TYPE,\r\n    INVOICE_TYPE,\r\n    GROSS_SALES,\r\n    DISCOUNT_AMOUNT,\r\n    NET_SALES,\r\n    QUANTITY,\r\n    COGS,\r\n    COST,\r\n    PULL_DATETIME,\r\n    PULL_DATETIME_UTC,\r\n    now() GLUE_PULL_DATETIME_UTC\r\nFROM\r\n    DAT.TDP_ALL_SALES\r\nWHERE\r\n    ACCOUNTING_DATE BETWEEN ADD_MONTHS(ADD_DAYS(LAST_DAY(CURRENT_DATE), 1), -2) AND ADD_DAYS(CURRENT_DATE,-1)"}, transformation_ctx="SAPHANA_node1717771614827")

# Script generated for node Amazon S3
AmazonS3_node1717772088502 = glueContext.write_dynamic_frame.from_options(frame=SAPHANA_node1717771614827, connection_type="s3", format="glueparquet", connection_options={"path": f"s3://{bucket_prefix}/tdp_all_sales/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1717772088502")

job.commit()