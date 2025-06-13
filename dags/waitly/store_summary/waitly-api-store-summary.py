import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models.baseoperator import chain

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'waitly_api_daily',
    default_args=default_args,
    description='Pull daily data from Waitly API',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['waitly', 'api', 'store_summary'],
    template_searchpath=['/home/ec2-user/airflow/dags/waitly/store_summary']
)

# Task to invoke the Lambda function
invoke_lambda = LambdaInvokeFunctionOperator(
    task_id='invoke_waitly_lambda',
    function_name='waitly-live-api-daily',  # Adjust to match your actual Lambda function name with environment suffix
    invocation_type='RequestResponse',  # Use 'Event' for asynchronous invocation
    payload=json.dumps({}),  # Empty payload as the Lambda doesn't require input
    aws_conn_id='aws_default',  # Use your Airflow AWS connection
    region='us-east-1',
    dag=dag,
)

# Task to create the raw data table
create_waitly_store_summary_raw = SnowflakeOperator(
    task_id='create_waitly_store_summary_raw',
    sql='create_waitly_store_summary_raw.sql',  
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task to create the structured data table
create_waitly_store_summary = SnowflakeOperator(
    task_id='create_waitly_store_summary',
    sql='create_waitly_store_summary.sql',  
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task to copy data using Snowflake COPY INTO
copy_waitly_store_summary = SnowflakeOperator(
    task_id='copy_waitly_store_summary',
    sql='copy_waitly_store_summary.sql', 
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task to execute the Snowflake transformation query
transform_waitly_store_summary = SnowflakeOperator(
    task_id='transform_waitly_store_summary',
    sql='transform_waitly_store_summary.sql',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task to cleanup old records from the summary table
cleanup_waitly_store_summary_tables = SnowflakeOperator(
    task_id='cleanup_waitly_store_summary',
    sql='cleanup_waitly_store_summary_tables.sql',
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Define DAG execution order using chain
chain(
    invoke_lambda,
    create_waitly_store_summary_raw,
    create_waitly_store_summary,
    copy_waitly_store_summary,
    transform_waitly_store_summary,
    cleanup_waitly_store_summary_tables
)
