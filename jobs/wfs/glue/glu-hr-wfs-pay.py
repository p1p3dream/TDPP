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
AmazonS3_node1728590252815 = glueContext.create_dynamic_frame.from_catalog(database="db_hr", table_name="wfs_delta_pay", transformation_ctx="AmazonS3_node1728590252815")

# Script generated for node SQL Query
SqlQuery8062 = '''
SELECT 
    COALESCE(recordid, '') AS RECORDID,
    COALESCE(CAST(sequence AS STRING), '') AS SEQUENCE,
    COALESCE(datachange, '') AS DATACHANGE,
    COALESCE(externalmatchid, '') AS EXTERNALMATCHID,
    COALESCE(periodstartdate, '') AS PERIODSTARTDATE,
    COALESCE(periodenddate, '') AS PERIODENDDATE,
    COALESCE(time_rec.amount, '') AS AMOUNT,
    COALESCE(time_rec.effectiverate, '') AS EFFECTIVERATE,
    COALESCE(time_rec.endtimestamp, '') AS ENDTIMESTAMP,
    COALESCE(time_rec.grosspay, '') AS GROSSPAY,
    COALESCE(time_rec.hours, '') AS HOURS,
    COALESCE(time_rec.jobid, '') AS JOBID,
    COALESCE(time_rec.paycode, '') AS PAYCODE,
    COALESCE(time_rec.paycurrencycode, '') AS PAYCURRENCYCODE,
    COALESCE(time_rec.payrollid, '') AS PAYROLLID,
    COALESCE(time_rec.recordtype, '') AS RECORDTYPE,
    COALESCE(time_rec.starttimestamp, '') AS STARTTIMESTAMP,
    COALESCE(time_rec.workdate, '') AS WORKDATE,
    COALESCE(time_rec.employeejob, '') AS EMPLOYEEJOB,
    COALESCE(CAST(time_rec.index AS STRING), '') AS INDEX,
    COALESCE(time_rec.additionalfields.c_ld2_cost_center_id, '') AS C_LD2_COST_CENTER_ID,
    COALESCE(time_rec.additionalfields.overlapping_rest_period, '') AS OVERLAPPING_REST_PERIOD,
    COALESCE(time_rec.additionalfields.adj_pay_rate, '') AS ADJ_PAY_RATE,
    COALESCE(time_rec.additionalfields.ec_wage_type, '') AS EC_WAGE_TYPE,
    COALESCE(time_rec.additionalfields.sp_earnings_code, '') AS SP_EARNINGS_CODE,
    COALESCE(time_rec.additionalfields.timesheet_totals, '') AS TIMESHEET_TOTALS
FROM 
    wfs_delta_pay
LATERAL VIEW EXPLODE(timerecords) AS time_rec;

'''
SQLQuery_node1728586347593 = sparkSqlQuery(glueContext, query = SqlQuery8062, mapping = {"wfs_delta_pay":AmazonS3_node1728590252815}, transformation_ctx = "SQLQuery_node1728586347593")

# Script generated for node SAP HANA
SAPHANA_node1721774326783 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1728586347593, connection_type="saphana", connection_options={"dbtable": "SC_HR.WFS_PAY", "connectionName": "Saphana connection"}, transformation_ctx="SAPHANA_node1721774326783")

job.commit()