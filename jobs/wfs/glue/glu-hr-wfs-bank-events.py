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
AWSGlueDataCatalog_node1734551002022 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_bank_events", transformation_ctx="AWSGlueDataCatalog_node1734551002022")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    COALESCE(datachange, '') AS datachange,
    COALESCE(externalmatchid, '') AS externalmatchid,
    COALESCE(eventdate, '') AS eventdate,
    COALESCE(recordid, '') AS recordid,
    COALESCE(sequence, '') AS sequence,
    COALESCE(bank.bankid, '') AS bank_id,
    COALESCE(bank.name, '') AS bank_name,
    COALESCE(bank.unit, '') AS bank_unit,
    COALESCE(event.employeejob, '') AS event_employee_job,
    COALESCE(event.externaljobid, '') AS event_external_job_id,
    COALESCE(event.amount, '') AS event_amount,
    COALESCE(event.previousbalance, '') AS event_previous_balance,
    COALESCE(event.type, '') AS event_type,
    COALESCE(event.paycodeid, '') AS event_paycode_id
FROM wfs_delta_bank_events
LATERAL VIEW EXPLODE(banks) AS bank
LATERAL VIEW EXPLODE(bank.events) AS event
WHERE datachange = 'MERGE'
UNION ALL
SELECT
    COALESCE(datachange, '') AS datachange,
    COALESCE(externalmatchid, '') AS externalmatchid,
    COALESCE(eventdate, '') AS eventdate,
    COALESCE(recordid, '') AS recordid,
    COALESCE(sequence, '') AS sequence,
    '' AS bank_id,
    '' AS bank_name,
    '' AS bank_unit,
    '' AS event_employee_job,
    '' AS event_external_job_id,
    '' AS event_amount,
    '' AS event_previous_balance,
    '' AS event_type,
    '' AS event_paycode_id
FROM wfs_delta_bank_events
WHERE datachange = 'DELETE';
'''
SQLQuery_node1734551004629 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"wfs_delta_bank_events":AWSGlueDataCatalog_node1734551002022}, transformation_ctx = "SQLQuery_node1734551004629")

# Script generated for node SAP HANA
SAPHANA_node1734551009438 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1734551004629, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_BANK_EVENTS", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1734551009438")

job.commit()