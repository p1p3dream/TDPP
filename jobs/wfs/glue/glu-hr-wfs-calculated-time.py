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
AWSGlueDataCatalog_node1728503777860 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_calculated_time", transformation_ctx="AWSGlueDataCatalog_node1728503777860")

# Script generated for node SQL Query
SqlQuery2765 = '''
SELECT 
    recordid AS RECORDID,
    sequence AS SEQUENCE,
    datachange AS DATACHANGE,
    externalmatchid AS EXTERNALMATCHID,
    workdate AS WORKDATE,
    COALESCE(t.amount, '') AS AMOUNT,
    COALESCE(t.effectiverate, '') AS EFFECTIVERATE,
    COALESCE(t.endtimestamp, '') AS ENDTIMESTAMP,
    COALESCE(t.grosspay, '') AS GROSSPAY,
    COALESCE(t.hours, '') AS HOURS,
    COALESCE(t.jobid, '') AS JOBID,
    COALESCE(t.paycode, '') AS PAYCODE,
    COALESCE(t.paycurrencycode, '') AS PAYCURRENCYCODE,
    COALESCE(t.payrollid, '') AS PAYROLLID,
    COALESCE(t.recordtype, '') AS RECORDTYPE,
    COALESCE(t.starttimestamp, '') AS STARTTIMESTAMP,
    COALESCE(t.employeejob, '') AS EMPLOYEEJOB,
    COALESCE(t.index, '') AS INDEX,
    COALESCE(t.additionalfields.adj_pay_rate, '') AS ADJ_PAY_RATE,
    COALESCE(t.additionalfields.c_ld2_cost_center_id, '') AS C_LD2_COST_CENTER_ID,
    COALESCE(t.additionalfields.ec_wage_type, '') AS EC_WAGE_TYPE,
    COALESCE(t.additionalfields.overlapping_rest_period, '') AS OVERLAPPING_REST_PERIOD,
    COALESCE(t.additionalfields.sp_earnings_code, '') AS SP_EARNINGS_CODE,
    COALESCE(t.additionalfields.timesheet_totals, '') AS TIMESHEET_TOTALS
FROM 
    wfs_delta_calculated_time
LATERAL VIEW EXPLODE(timerecords) AS t;

'''
SQLQuery_node1728580660860 = sparkSqlQuery(glueContext, query = SqlQuery2765, mapping = {"wfs_delta_calculated_time":AWSGlueDataCatalog_node1728503777860}, transformation_ctx = "SQLQuery_node1728580660860")

# Script generated for node Change Schema
ChangeSchema_node1728503614893 = ApplyMapping.apply(frame=SQLQuery_node1728580660860, mappings=[("RECORDID", "string", "RECORDID", "string"), ("SEQUENCE", "int", "SEQUENCE", "int"), ("DATACHANGE", "string", "DATACHANGE", "string"), ("EXTERNALMATCHID", "string", "EXTERNALMATCHID", "string"), ("WORKDATE", "string", "WORKDATE", "string"), ("AMOUNT", "string", "AMOUNT", "string"), ("EFFECTIVERATE", "string", "EFFECTIVERATE", "string"), ("ENDTIMESTAMP", "string", "ENDTIMESTAMP", "string"), ("GROSSPAY", "string", "GROSSPAY", "string"), ("HOURS", "string", "HOURS", "string"), ("JOBID", "string", "JOBID", "string"), ("PAYCODE", "string", "PAYCODE", "string"), ("PAYCURRENCYCODE", "string", "PAYCURRENCYCODE", "string"), ("PAYROLLID", "string", "PAYROLLID", "string"), ("RECORDTYPE", "string", "RECORDTYPE", "string"), ("STARTTIMESTAMP", "string", "STARTTIMESTAMP", "string"), ("EMPLOYEEJOB", "string", "EMPLOYEEJOB", "string"), ("INDEX", "string", "INDEX", "int"), ("ADJ_PAY_RATE", "string", "ADJ_PAY_RATE", "string"), ("C_LD2_COST_CENTER_ID", "string", "C_LD2_COST_CENTER_ID", "string"), ("EC_WAGE_TYPE", "string", "EC_WAGE_TYPE", "string"), ("OVERLAPPING_REST_PERIOD", "string", "OVERLAPPING_REST_PERIOD", "string"), ("SP_EARNINGS_CODE", "string", "SP_EARNINGS_CODE", "string"), ("TIMESHEET_TOTALS", "string", "TIMESHEET_TOTALS", "string")], transformation_ctx="ChangeSchema_node1728503614893")

# Script generated for node SAP HANA
SAPHANA_node1721776225072 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1728503614893, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_CALCULATED_TIME", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1721776225072")

job.commit()