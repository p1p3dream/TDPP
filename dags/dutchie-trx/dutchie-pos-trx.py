import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

initial_payload = '''{
  "endpoints": [
    "reporting/transactions"
  ],
  "parameters": {
    "reporting/transactions": {
      "IncludeDetail": true,
      "IncludeTaxes": true,
      "IncludeOrderIds": true,
      "IncludeFeesAndDonations": true
    }
  },
  "send_to_sqs": true,
  "reprocess": false,
  "force_update": false
}'''

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dutchie-pos-transactions-load',
    default_args=default_args,
    description='Pull daily data from Dutchie API',
    schedule='*/5 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dutchie', 'api', 'pos', 'transactions'],
    template_searchpath=['/home/ec2-user/airflow/dags/waitly/store_summary'],
) as dag:

    ingest_to_raw = LambdaInvokeFunctionOperator(
        task_id='lam-ingestion-raw-dutchiepos-reporting',
        function_name='lam-ingestion-raw-dutchiepos-reporting-16367',
        invocation_type='RequestResponse',
        payload=initial_payload,
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )

    raw_to_stage = LambdaInvokeFunctionOperator(
        task_id='lam-ingestion-stg-dutchiepos-reporting-transactions',
        function_name='lam-ingestion-stg-dutchiepos-reporting-transactions-16367',
        invocation_type='RequestResponse',
        payload="{{ ti.xcom_pull(task_ids='lam-ingestion-raw-dutchiepos-reporting') | from_json()['output_messages'] }}",
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )

    stage_to_bronze = LambdaInvokeFunctionOperator(
        task_id='lam-ingestion-bronze-dutchiepos-reporting',
        function_name='lam-ingestion-bronze-dutchiepos-reporting-16367',
        invocation_type='RequestResponse',
        payload=json.dumps({}),
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )

    ingest_to_raw >> raw_to_stage >> stage_to_bronze