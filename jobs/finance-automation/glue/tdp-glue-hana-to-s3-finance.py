# Import required libraries
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters from the Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'start_date', 'end_date', 'report_name', 'state', 'finance-bucket'])
start_date = args['start_date']
end_date = args['end_date']
state = args['state']
report_name = args['report_name']
finance_bucket = args['finance_bucket']

# Initialize Glue job components
# Create a Spark context and Glue context for data processing
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Fetch SQL query from S3
# The query is stored in a .sql file in the specified S3 bucket
s3 = boto3.client('s3')
try:
    response = s3.get_object(Bucket=finance_bucket, Key=f'00-report-querys/{report_name}.sql')
    query = response['Body'].read().decode('utf-8')
except Exception as e:
    print(f"Error fetching SQL query from S3: {e}")
    raise

# Replace placeholders in the query with actual date parameters
query = query.replace('{start_date}', start_date).replace('{end_date}', end_date)

# Process data based on state parameter
if state != "NA":
    # If state is not NA, split the comma-separated states and process each one
    states = state.split(',')
    
    for single_state in states:
        # Replace the state placeholder in the query for each state
        state_query = query.replace('{state}', single_state)
        
        # Execute the query against the SAP HANA database for the current state
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="saphana",
            connection_options={
                "connectionName": "Saphana connection",
                "query": state_query
            },
            transformation_ctx=f"read_saphana_{single_state}"
        )
        
        # Convert the DynamicFrame to a DataFrame for easier handling
        df = dynamic_frame.toDF()
        file_name = f"report-{single_state}-{start_date}-to-{end_date}.csv"
        
        # Create a temporary path for initial file writing
        # This prevents conflicts when processing multiple states
        temp_path = f"s3://{finance_bucket}/{report_name}/temp_{single_state}"
        
        # Write the DataFrame to the temporary path
        # coalesce(1) ensures a single output file
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
        
        # Initialize S3 resource for file operations
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(finance_bucket)
        
        # Find the temporary file and move it to the final destination
        for obj in bucket.objects.filter(Prefix=f"{report_name}/temp_{single_state}/part-00000-"):
            old_key = obj.key
            new_key = f"{report_name}/{file_name}"
            
            # Copy the file to its final destination with the correct name
            bucket.Object(new_key).copy_from(CopySource={'Bucket': finance_bucket, 'Key': old_key})
            
            # Clean up temporary files
            for temp_obj in bucket.objects.filter(Prefix=f"{report_name}/temp_{single_state}/"):
                temp_obj.delete()

else:
    # If state is NA, execute the query once without state filtering
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="saphana",
        connection_options={
            "connectionName": "Saphana connection",
            "query": query
        },
        transformation_ctx="read_saphana"
    )
    
    # Process the results similarly to the state-specific case
    df = dynamic_frame.toDF()
    file_name = f"report-{start_date}-to-{end_date}.csv"
    
    temp_path = f"s3://{finance_bucket}/{report_name}/temp"
    
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(finance_bucket)
    
    # Move the file from temporary location to final destination
    for obj in bucket.objects.filter(Prefix=f"{report_name}/temp/part-00000-"):
        old_key = obj.key
        new_key = f"{report_name}/{file_name}"
        
        bucket.Object(new_key).copy_from(CopySource={'Bucket': finance_bucket, 'Key': old_key})
        
        # Clean up temporary files
        for temp_obj in bucket.objects.filter(Prefix=f"{report_name}/temp/"):
            temp_obj.delete()

# Commit the job
job.commit()
print(f"CSV files successfully saved to: s3://{finance_bucket}/{report_name}/")
