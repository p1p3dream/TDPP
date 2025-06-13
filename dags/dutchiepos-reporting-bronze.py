from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import boto3
import pandas as pd
import awswrangler as wr

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID = 'snowflake_default'
S3_CONN_ID = 'aws_default'
S3_BUCKET = 'your-bucket'
S3_PREFIX = 'path/to/monitor/'

def process_s3_file(**context):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    ti = context['task_instance']
    s3_key = ti.xcom_pull(task_ids='wait_for_file')
    
    obj = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET)
    json_data = json.loads(obj.get()['Body'].read().decode('utf-8'))
    
    table_name = s3_key.split('/')[2]
    source_system = s3_key.split('/')[1]
    file_name = s3_key.split('/')[-1][:-5]
    
    metadata, response = get_metadata(json_data)
    json_denorm = denormalize(response, table_name)
    
    processed_data = []
    client_id = metadata.get('clientid', 'unknown')
    
    for table in json_denorm:
        frame = pd.DataFrame.from_dict(json_denorm[table])
        s3_path = f's3://tdp-bronze/{source_system}/{table}/{client_id}/{file_name}.parquet'
        frame['s3_file'] = s3_path
        
        if table == table_name:
            for col in metadata:
                frame[col] = metadata[col]
        else:
            frame['clientid'] = client_id
            frame['last_processed_datetime_utc'] = metadata['last_processed_datetime_utc']
        
        processed_data.append({
            'table': table,
            'data': frame,
            'path': s3_path,
            'bronze_file_name': f"{source_system}/{table}/{client_id}/{file_name}.parquet",
            'snowflake_table': f"{source_system}_{table}".upper()
        })
    
    return processed_data

def write_to_s3(**context):
    ti = context['task_instance']
    processed_data = ti.xcom_pull(task_ids='process_s3_file')
    session = boto3.session.Session()
    
    for table_data in processed_data:
        wr.s3.to_parquet(
            df=table_data['data'],
            path=table_data['path'],
            index=False,
            boto3_session=session,
            compression='snappy'
        )
        
        ti.xcom_push(
            key=f"load_config_{table_data['table']}", 
            value={
                'table': table_data['snowflake_table'],
                'file': table_data['bronze_file_name']
            }
        )

def generate_copy_sql(**context):
    ti = context['task_instance']
    table_data = context['task_instance'].xcom_pull(
        task_ids='write_to_s3', 
        key=f"load_config_{context['task'].task_id.replace('copy_to_snowflake_', '')}"
    )
    
    return f"""
    CREATE TABLE IF NOT EXISTS TDP_DEV.BRONZE.{table_data['table']}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION=>'@TDP_DEV.BRONZE.AWS_S3_STAGE_BRONZE/{table_data["file"]}',
                FILE_FORMAT=>'tdp_parquet_format'
            )
        )
    );
    
    COPY INTO TDP_DEV.BRONZE.{table_data['table']}
    FROM '@TDP_DEV.BRONZE.AWS_S3_STAGE_BRONZE/{table_data["file"]}'
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = case_insensitive
    FORCE = TRUE;
    """

dag = DAG(
    'snowflake_etl',
    default_args=default_args,
    description='ETL pipeline for loading data to Snowflake',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    tags=['snowflake', 'etl'],
)

wait_for_file = S3KeySensor(
    task_id='wait_for_file',
    bucket_key=f'{S3_PREFIX}*.json',
    wildcard_match=True,
    bucket_name=S3_BUCKET,
    aws_conn_id=S3_CONN_ID,
    timeout=60 * 60,  # 1 hour
    poke_interval=60,  # Check every minute
    mode='poke',
    dag=dag,
)

process_file = PythonOperator(
    task_id='process_s3_file',
    python_callable=process_s3_file,
    dag=dag,
)

write_s3 = PythonOperator(
    task_id='write_to_s3',
    python_callable=write_to_s3,
    dag=dag,
)

tables_to_load = ['transactions', 'transactions_items', 'products']
for table in tables_to_load:
    copy_task = SnowflakeOperator(
        task_id=f'copy_to_snowflake_{table}',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=generate_copy_sql,
        dag=dag,
    )
    wait_for_file >> process_file >> write_s3 >> copy_task