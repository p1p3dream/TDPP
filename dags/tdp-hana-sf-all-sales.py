from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import boto3
import logging
from airflow.models.baseoperator import chain

# Set up logging
logger = logging.getLogger(__name__)

def log_connection_info(**context):
    """Log Snowflake connection information"""
    from airflow.hooks.base import BaseHook
    
    try:
        # Get Snowflake connection
        conn = BaseHook.get_connection('snowflake_default')
        logger.info(f"Connection User: {conn.login}")
        logger.info(f"Connection Schema: {conn.schema}")
        logger.info(f"Connection Extra: {conn.extra_dejson}")
        
        # Get current Airflow user
        airflow_user = context['task_instance'].task.owner
        logger.info(f"Airflow Task Owner: {airflow_user}")
        
    except Exception as e:
        logger.error(f"Error getting connection info: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60)
}

dag = DAG(
    'tdp_sales_s3_to_snowflake',
    default_args=default_args,
    description='Load TDP sales data from HANA to S3 to Snowflake',
    schedule_interval='0 6 * * *',
    catchup=False
)

# Add connection info logging task
log_conn_info = PythonOperator(
    task_id='log_connection_info',
    python_callable=log_connection_info,
    provide_context=True,
    dag=dag
)

# Glue job to extract from HANA to S3
extract_to_s3 = GlueJobOperator(
    task_id='extract_hana_to_s3',
    job_name='dev-hana-s3-gold-tdp-all-sales-openperiods',
    aws_conn_id='aws_default',
    wait_for_completion=True,
    dag=dag
)


# Stage data in Snowflake
stage_data = SnowflakeOperator(
    task_id='stage_s3_data',
    snowflake_conn_id='snowflake_default',
    sql='{% include "./dag_queries/tdp_all_sales/stage_all_sales.sql" %}',
    dag=dag
)

# Merge staged data into target table
merge_data = SnowflakeOperator(
    task_id='merge_to_target',
    snowflake_conn_id='snowflake_default',
    sql='{% include "./dag_queries/tdp_all_sales/merge_tdp_all_sales.sql" %}',
    dag=dag
)

# Set task dependencies using chain
chain(
    log_conn_info,
    extract_to_s3,
    stage_data,
    merge_data
)