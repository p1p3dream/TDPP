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
AWSGlueDataCatalog_node1721776161794 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_time_off", transformation_ctx="AWSGlueDataCatalog_node1721776161794")

# Script generated for node SQL Query
SqlQuery3451 = '''
SELECT 
    CAST(recordid AS STRING) AS RECORDID,
    CAST(sequence AS STRING) AS SEQUENCE,
    CAST(datachange AS STRING) AS DATACHANGE,
    CAST(timeoffid AS STRING) AS TIMEOFFID,
    CAST(externalmatchid AS STRING) AS EXTERNALMATCHID,
    COALESCE(CAST(externaljobid AS STRING), '') AS EXTERNALJOBID,
    COALESCE(CAST(enddate AS STRING), '') AS ENDDATE,
    COALESCE(CAST(requestmadeat AS STRING), '') AS REQUESTMADEAT,
    COALESCE(CAST(startdate AS STRING), '') AS STARTDATE,
    COALESCE(CAST(status AS STRING), '') AS STATUS,
    COALESCE(CAST(timeofftype AS STRING), '') AS TIMEOFFTYPE,
    COALESCE(CAST(d.codeid AS STRING), '') AS CODEID,
    COALESCE(CAST(d.date AS STRING), '') AS DATE,
    COALESCE(CAST(d.enddatetime AS STRING), '') AS ENDDATETIME,
    COALESCE(CAST(d.quantity AS STRING), '') AS QUANTITY,
    COALESCE(CAST(d.startdatetime AS STRING), '') AS STARTDATETIME,
    COALESCE(CAST(d.unit AS STRING), '') AS UNIT,
    COALESCE(CAST(d.numberformatoptions.currencycode AS STRING), '') AS CURRENCYCODE,
    COALESCE(CAST(d.numberformatoptions.maximumfractiondigits AS STRING), '') AS MAXIMUMFRACTIONDIGITS,
    COALESCE(CAST(d.numberformatoptions.minimumfractiondigits AS STRING), '') AS MINIMUMFRACTIONDIGITS,
    COALESCE(CAST(d.numberformatoptions.minimumintegerdigits AS STRING), '') AS MINIMUMINTEGERDIGITS,
    COALESCE(CAST(d.numberformatoptions.preventgrouping AS STRING), '') AS PREVENTGROUPING,
    COALESCE(CAST(d.numberformatoptions.style AS STRING), '') AS STYLE,
    COALESCE(CAST(d.numberformatoptions.useaccountingstylecurrencysign AS STRING), '') AS USEACCOUNTINGSTYLECURRENCYSIGN
FROM 
    wfs_delta_time_off
LATERAL VIEW EXPLODE(details) AS d;
'''
SQLQuery_node1730206560252 = sparkSqlQuery(glueContext, query = SqlQuery3451, mapping = {"wfs_delta_time_off":AWSGlueDataCatalog_node1721776161794}, transformation_ctx = "SQLQuery_node1730206560252")

# Script generated for node SAP HANA
SAPHANA_node1721776165493 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1730206560252, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_TIME_OFF", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1721776165493")

job.commit()