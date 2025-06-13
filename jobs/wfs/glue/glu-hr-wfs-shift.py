import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType
from awsglue import DynamicFrame

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from Amazon S3
AmazonS3_node = glueContext.create_dynamic_frame.from_catalog(
    database="db_hr", 
    table_name="wfs_delta_shift", 
    transformation_ctx="AmazonS3_node"
)

# Filter records with dataChange in ['MERGE', 'DELETE']
Filtered_node = Filter.apply(
    frame=AmazonS3_node, 
    f=lambda row: row["dataChange"] in ["MERGE", "DELETE"], 
    transformation_ctx="Filtered_node"
)

# Convert to DataFrame for easier handling
df = Filtered_node.toDF()

# Flatten 'shifts' array for both MERGE and DELETE
df = df.withColumn("shift", F.explode_outer("shifts"))

# Flatten 'activities' and 'breaks' within each shift for MERGE (DELETE will result in NULL fields)
df = df.withColumn("activity", F.explode_outer(F.col("shift.activities")))
df = df.withColumn("break", F.explode_outer(F.col("shift.breaks")))

# Extract relevant fields with explicit casting, handling NULLs for DELETE records
df = df.select(
    F.col("recordId").alias("RECORDID"),
    F.col("sequence").cast(IntegerType()).alias("SEQUENCE"),
    F.col("dataChange").alias("DATACHANGE"),
    F.col("workDate").alias("WORKDATE"),
    F.col("externalMatchId").alias("EXTERNALMATCHID"),
    F.col("shift.shiftId").alias("SHIFTID"),
    F.col("shift.signature").alias("SIGNATURE"),
    F.col("shift.durationInSeconds").cast(LongType()).alias("DURATIONINSECONDS"),
    F.col("shift.status").alias("STATUS"),
    F.col("shift.startAt").alias("STARTAT"),
    F.col("shift.endAt").alias("ENDAT"),
    F.col("shift.employeeJob").alias("EMPLOYEEJOB"),
    F.col("activity.activityId").alias("ACTIVITYID"),
    F.col("activity.startAt").alias("ACTIVITYSTARTAT"),
    F.col("activity.endAt").alias("ACTIVITYENDAT"),
    F.col("activity.taskId").alias("TASKID"),
    F.col("activity.task").alias("TASK"),
    F.col("activity.locationId").alias("LOCATIONID"),
    F.col("activity.location").alias("LOCATION"),
    F.col("activity.taskCode").alias("TASKCODE"),
    F.col("activity.locationCode").alias("LOCATIONCODE"),
    F.col("activity.durationInSeconds").cast(LongType()).alias("ACTIVITYDURATIONINSECONDS"),
    F.col("break.breakId").alias("BREAKID"),
    F.col("break.startAt").alias("BREAKSTARTAT"),
    F.col("break.endAt").alias("BREAKENDAT"),
    F.col("break.durationInSeconds").cast(LongType()).alias("BREAKDURATIONINSECONDS"),
    F.col("break.isPaid").alias("ISPAID")
)

# Convert back to DynamicFrame for Glue compatibility
Flattened_filled_node = DynamicFrame.fromDF(df, glueContext, "Flattened_filled_node")

# Define the schema mapping for the target SAP HANA table
mappings = [
    ("RECORDID", "string", "RECORDID", "string"),
    ("SEQUENCE", "int", "SEQUENCE", "string"),
    ("DATACHANGE", "string", "DATACHANGE", "string"),
    ("WORKDATE", "string", "WORKDATE", "string"),
    ("EXTERNALMATCHID", "string", "EXTERNALMATCHID", "string"),
    ("SHIFTID", "string", "SHIFTID", "string"),
    ("SIGNATURE", "string", "SIGNATURE", "string"),
    ("DURATIONINSECONDS", "long", "DURATIONINSECONDS", "string"),
    ("STATUS", "string", "STATUS", "string"),
    ("STARTAT", "string", "STARTAT", "string"),
    ("ENDAT", "string", "ENDAT", "string"),
    ("EMPLOYEEJOB", "string", "EMPLOYEEJOB", "string"),
    ("ACTIVITYID", "string", "ACTIVITYID", "string"),
    ("ACTIVITYSTARTAT", "string", "ACTIVITYSTARTAT", "string"),
    ("ACTIVITYENDAT", "string", "ACTIVITYENDAT", "string"),
    ("TASKID", "string", "TASKID", "string"),
    ("TASK", "string", "TASK", "string"),
    ("LOCATIONID", "string", "LOCATIONID", "string"),
    ("LOCATION", "string", "LOCATION", "string"),
    ("TASKCODE", "string", "TASKCODE", "string"),
    ("LOCATIONCODE", "string", "LOCATIONCODE", "string"),
    ("ACTIVITYDURATIONINSECONDS", "long", "ACTIVITYDURATIONINSECONDS", "string"),
    ("BREAKID", "string", "BREAKID", "string"),
    ("BREAKSTARTAT", "string", "BREAKSTARTAT", "string"),
    ("BREAKENDAT", "string", "BREAKENDAT", "string"),
    ("BREAKDURATIONINSECONDS", "long", "BREAKDURATIONINSECONDS", "string"),
    ("ISPAID", "boolean", "ISPAID", "string")
]

# Apply the schema mapping to the flattened DynamicFrame
Mapped_node = ApplyMapping.apply(
    frame=Flattened_filled_node, 
    mappings=mappings, 
    transformation_ctx="Mapped_node"
)

# Write to SAP HANA
glueContext.write_dynamic_frame.from_options(
    frame=Mapped_node, 
    connection_type="saphana", 
    connection_options={
        "dbtable": "SC_HR.WFS_SHIFT", 
        "connectionName": "Saphana connection"
    }, 
    transformation_ctx="SAPHANA_node"
)

job.commit()