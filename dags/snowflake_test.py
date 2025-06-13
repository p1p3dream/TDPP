from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='snowflake_test_dag',
    default_args=default_args,
    description='A simple DAG to test Snowflake connection',
    schedule_interval=None,  # Manually triggered
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define a task using SnowflakeOperator to execute a basic query
    snowflake_query = SnowflakeOperator(
        task_id='snowflake_test_query',
        snowflake_conn_id='snowflake_http',  
        sql="SELECT * FROM CONSUMERS LIMIT 5;",
    )

    # The DAG's task chain
    snowflake_query
