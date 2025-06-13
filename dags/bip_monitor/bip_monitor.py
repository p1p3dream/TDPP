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
    'lam-saphana-bip-monitor',
    default_args=default_args,
    description='Pull daily data from Waitly API',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bip', 'hana', 'monitor'],
    #template_searchpath=['/home/ec2-user/airflow/dags/waitly/store_summary']
)

# Task to invoke the Lambda function
invoke_lambda_bip_monitor = LambdaInvokeFunctionOperator(
    task_id='invoke_lam-saphana-bip-monitor',
    function_name='lam-saphana-bip-monitor',  # Adjust to match your actual Lambda function name with environment suffix
    invocation_type='RequestResponse',  # Use 'Event' for asynchronous invocation
    payload=json.dumps({}),  # Empty payload as the Lambda doesn't require input
    aws_conn_id='aws_default',  # Use your Airflow AWS connection
    region='us-east-1',
    dag=dag,
)

chain(
  invoke_lambda_bip_monitor
)