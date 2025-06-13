from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ga4_bigquery_events_analytics-bq-gold',
    default_args=default_args,
    start_date=days_ago(90),
    schedule_interval=timedelta(days=1),
    catchup=False
)

def get_date_range(**context):
    dag_run = context['dag_run']
    
    if dag_run.external_trigger:  # Manual run
        start_date = datetime(2024, 9, 1)  # Adjust this date as needed
        end_date = context['data_interval_end']
    else:  # Scheduled run
        start_date = context['data_interval_start']
        end_date = context['data_interval_end']
    
    return {
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    }

task_get_date_range = PythonOperator(
    task_id='get_date_range',
    python_callable=get_date_range,
    provide_context=True,
    dag=dag,
)

# Store start and end dates in variables
start_date = "{{ task_instance.xcom_pull(task_ids='get_date_range', key='start_date') }}"
end_date = "{{ task_instance.xcom_pull(task_ids='get_date_range', key='end_date') }}"

print(f"********* Start Date: {start_date} ")
print(f"********* End Date: {end_date} ")


# Source to Bronze
task_glue_source_to_bronze = GlueJobOperator(
    task_id='glue_source_to_bronze',
    job_name='mwaa-ga4-bq-bronze',
    region_name='us-east-1',
    # script_args={
    #     '--start_date': start_date,
    #     '--end_date': end_date
    # },
    # dag=dag,
)


# Bronze to Silver 1
task_glue_bronze_to_silver1 = GlueJobOperator(
    task_id='glue_bronze_to_silver1',
    job_name='mwaa-ga4-events-bronze-silver1',
    region_name='us-east-1',
    # script_args={
    #     '--start_date': start_date,
    #     '--end_date': end_date
    # },
    dag=dag,
)

# Silver 1 to Silver 2
task_glue_silver1_to_silver2 = GlueJobOperator(
    task_id='glue_silver1_to_silver2',
    job_name='glue-tdp-ecommerce-events-analytics_408060011-silver1-to-silver2',
    region_name='us-east-1',
    script_args={
        '--start_date': start_date,
        '--end_date': end_date
    },
    dag=dag,
)

# Silver to Gold
task_glue_silver_to_gold = GlueJobOperator(
    task_id='glue_silver_to_gold',
    job_name='glue-tdp-ecommerce-session-analytics_408060011-gold',
    region_name='us-east-1',
    script_args={
        '--start_date': start_date,
        '--end_date': end_date
    },
    dag=dag,
)

task_glue_gold_to_redshift = GlueJobOperator(
    task_id='glue_gold_to_redshift',
    job_name='glue-tdp-ecommerce-session-analytics_408060011-gold-to-redshift',
    region_name='us-east-1',
    script_args={
        '--start_date': start_date,
        '--end_date': end_date
    },
    dag=dag,
)

# Set up task dependencies


chain(
    task_get_date_range,
    task_glue_source_to_bronze,
    task_glue_bronze_to_silver1,
    # task_glue_silver1_to_silver2,
    # task_glue_silver_to_gold,
    # task_glue_gold_to_redshift
)


