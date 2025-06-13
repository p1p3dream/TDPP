from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import pandas as pd
import requests
from io import StringIO
import boto3

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
}

dag = DAG(
    'sap_hana_to_s3_dag_http',
    default_args=default_args,
    description='Extract data from SAP HANA using HTTP connection and load to S3',
    schedule_interval=None,
    catchup=False
)

# Define Python function for extracting data from SAP HANA via HTTP
def extract_data_from_sap_hana_http(**kwargs):
    # Fetch the Airflow connection details
    connection = BaseHook.get_connection('your_connection_id')  # Replace with your actual connection ID

    # SAP HANA connection details from Airflow's HTTP connection
    hana_url = f"https://{connection.host}"
    username = connection.login
    password = connection.password

    # Define the query
    query = """
    SELECT
        DATA_TYPE,
        SOURCE_SYSTEM,
        SOURCE_SYSTEM_ID,
        UNIFIED_STORE_ID,
        ORDER_ID,
        ORDER_ITEM_ID,
        ORDER_CREATED_DATETIME,
        ORDER_COMPLETED_DATETIME,
        ORDER_CREATED_DATETIME_UTC,
        ORDER_COMPLETED_DATETIME_UTC,
        ACCOUNTING_DATE,
        ORDER_CREATED_BY,
        ORDER_COMPLETED_BY,
        ORDER_FULFILLMENT_METHOD,
        MRW_FLAG,
        ECOMM_ORDER_ID,
        CUSTOMER_ID,
        UNIFIED_CUSTOMER_ID,
        PRODUCT_ID,
        UNIFIED_PRODUCT_ID,
        BATCH,
        ORDER_TYPE,
        INVOICE_TYPE,
        GROSS_SALES,
        DISCOUNT_AMOUNT,
        NET_SALES,
        QUANTITY,
        COGS,
        COST,
        PULL_DATETIME,
        PULL_DATETIME_UTC,
        now() AS GLUE_PULL_DATETIME_UTC
    FROM
        DAT.TDP_ALL_SALES
    WHERE
        ACCOUNTING_DATE >= '2023-01-01'
    """
    
    # Send the request to SAP HANA
    response = requests.post(
        hana_url,
        data={'query': query},  # Assuming the query needs to be in the data payload
        auth=(username, password)
    )
    
    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"Failed to query SAP HANA: {response.content}")
    
    # Read the response into a DataFrame
    df = pd.read_csv(StringIO(response.text))
    
    # Save the DataFrame as a local Parquet file
    file_path = '/tmp/data.parquet'
    df.to_parquet(file_path, index=False, compression='snappy')
    
    return file_path

# Define Python function for uploading data to S3
def load_data_to_s3(file_path):
    # Upload to S3
    s3 = boto3.client('s3')
    with open(file_path, 'rb') as f:
        s3.put_object(
            Bucket='tdp-bronze',
            Key='tdp_all_sales/data.parquet',
            Body=f
        )

# Task to extract data from SAP HANA
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_sap_hana_http,
    provide_context=True,
    dag=dag
)

# Task to load data to S3
load_task = PythonOperator(
    task_id='load_data',
    python_callable=lambda: load_data_to_s3(extract_task.output),
    dag=dag
)

# Set task dependencies
extract_task >> load_task