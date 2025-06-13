import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1734038796226 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_bank", transformation_ctx="AmazonS3_node1734038796226")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    CAST(COALESCE(recordid, '') AS STRING) AS recordid,
    CAST(COALESCE(sequence, 0) AS STRING) AS sequence,
    CAST(COALESCE(datachange, '') AS STRING) AS datachange,
    CAST(COALESCE(externalmatchid, '') AS STRING) AS externalmatchid,
    CAST(COALESCE(bankid, '') AS STRING) AS bankid,
    CAST(COALESCE(startat, '') AS STRING) AS startat,
    CAST(COALESCE(endat, '') AS STRING) AS endat,
    CAST(COALESCE(name, '') AS STRING) AS name,
    CAST(COALESCE(unit, '') AS STRING) AS unit,
    CAST(COALESCE(b.date, '') AS STRING) AS balance_date,
    CAST(COALESCE(b.balance, '') AS STRING) AS balance_amount,
    CAST(COALESCE(b.employeejob, '') AS STRING) AS balance_employeejob,
    CAST(COALESCE(generatedat, '') AS STRING) AS generatedat
FROM 
    wfs_delta_bank t 
LATERAL VIEW EXPLODE(t.balances) AS b;
'''
SQLQuery_node1734038844988 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"wfs_delta_bank":AmazonS3_node1734038796226}, transformation_ctx = "SQLQuery_node1734038844988")

# Script generated for node SAP HANA
SAPHANA_node1734039919519 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734038844988, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_BANK", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1734039919519")

job.commit()