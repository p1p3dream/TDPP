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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1722634456062 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_time_exceptions", transformation_ctx="AWSGlueDataCatalog_node1722634456062")

# Script generated for node SQL Query
SqlQuery4158 = '''
SELECT
    CAST(recordid AS STRING) AS RECORDID,
    CAST(sequence AS INT) AS SEQUENCE,
    CAST(datachange AS STRING) AS DATACHANGE,
    COALESCE(CAST(employeejob AS STRING), '') AS EMPLOYEEJOB,
    COALESCE(CAST(externalmatchid AS STRING), '') AS EXTERNALMATCHID,
    COALESCE(CAST(periodstartat AS STRING), '') AS PERIODSTARTAT,
    COALESCE(CAST(periodendat AS STRING), '') AS PERIODENDAT,
    COALESCE(CAST(exception.exceptioncode AS STRING), '') AS EXCEPTIONCODE,
    COALESCE(CAST(exception.logicalkey AS STRING), '') AS LOGICALKEY,
    COALESCE(CAST(exception.overridden AS STRING), 'false') AS OVERRIDDEN,
    COALESCE(CAST(exception.ruleid AS STRING), '') AS RULEID,
    COALESCE(CAST(exception.severity AS STRING), '') AS SEVERITY,
    COALESCE(CAST(exception.shortdescriptiontranslationkey AS STRING), '') AS SHORTDESCRIPTIONTRANSLATIONKEY,
    COALESCE(CAST(exception.showinassistant AS STRING), 'false') AS SHOWINASSISTANT,
    COALESCE(CAST(exception.titletranslationkey AS STRING), '') AS TITLETRANSLATIONKEY,
    COALESCE(CAST(exception.workdate AS STRING), '') AS WORKDATE
FROM 
    wfs_delta_time_exceptions
LATERAL VIEW EXPLODE(exceptions) t AS exception;

'''
SQLQuery_node1730466880654 = sparkSqlQuery(glueContext, query = SqlQuery4158, mapping = {"wfs_delta_time_exceptions":AWSGlueDataCatalog_node1722634456062}, transformation_ctx = "SQLQuery_node1730466880654")

# Script generated for node SAP HANA
SAPHANA_node1722634463814 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1730466880654, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_TIME_EXCEPTIONS", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1722634463814")

job.commit()