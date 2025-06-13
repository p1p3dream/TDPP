from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import json
import requests
import boto3
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_aws_session():
    aws_conn = BaseHook.get_connection('aws_innovation')
    return boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name='us-east-1'
    )
# Initialize clients for S3, DynamoDB, and SQS
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sqs_client = boto3.client('sqs')

# S3 and SQS configurations
BUCKET_NAME = "tru-datalake"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/590184123339/sqs-dutchie-pos-reporting"

def get_api_keys(table_name):
    api_keys = {}
    table = dynamodb.Table(table_name)
    response = table.scan()
    for item in response['Items']:
        api_keys[item['DUTCHIEPOS_ID']] = item['DUCHIEPOS_API_KEY']
    return api_keys

def get_last_processed_date(endpoint):
    table = dynamodb.Table('dutchiepos_endpoints_last_processed')
    response = table.get_item(Key={'endpoint': endpoint})
    if 'Item' in response:
        return response['Item'].get('last_runtime', None)
    return None

def save_last_processed_date(endpoint, last_runtime):
    table = dynamodb.Table('dutchiepos_endpoints_last_processed')
    table.put_item(Item={'endpoint': endpoint, 'last_runtime': last_runtime})

def save_data_to_s3(bucket_name, endpoint, store_id, data):
    timestamp = datetime.utcnow().strftime('%Y%m%d.%H%M%S')
    year = datetime.utcnow().strftime('%Y')
    month = datetime.utcnow().strftime('%m')
    day = datetime.utcnow().strftime('%d')
    
    filename = f"{endpoint.replace('/', '.')}.{store_id}.{timestamp}.json"
    s3_key = f'raw/dutchie-pos/sales/{year}/{month}/{day}/{store_id}/{filename}'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(data))
    print(f'[INFO] Data saved to s3://{bucket_name}/{s3_key}')

def send_all_to_sqs(queue_url, transactions, store_id):
    for transaction in transactions:
        transaction['storeId'] = store_id
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(transaction)
        )
    print(f'[INFO] Data sent to SQS queue: {queue_url}')

def fetch_and_process_data(api_url, endpoint, headers, params, store_id, send_to_sqs):
    try:
        print(f"[INFO] Requesting data for store ID {store_id} from {api_url}")
        print(f"[INFO] Parameters: {params}")
        
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data:
                total_records = len(data)
                print(f"[INFO] Data retrieved for store ID {store_id}: {total_records} records")
                
                if total_records > 0:
                    save_data_to_s3(BUCKET_NAME, endpoint, store_id, data)
                
                if total_records > 0 and send_to_sqs:
                    send_all_to_sqs(SQS_QUEUE_URL, data, store_id)
                
                print(f"[INFO] Total records processed for store ID {store_id}: {total_records}")
                
                save_last_processed_date(endpoint, params['ToLastModifiedDateUTC'])
                
                return {"status": "success", "store_id": store_id, "records_fetched": total_records}
            else:
                print(f"[INFO] No data returned for {endpoint} at store {store_id}.")
                return {"status": "no_data", "store_id": store_id}
        else:
            print(f"[ERROR] Failed to fetch data for {endpoint} at store {store_id}: {response.status_code} {response.text}")
            return {"status": "error", "store_id": store_id, "error_message": response.text}
    except Exception as e:
        print(f"[ERROR] Exception during processing for store ID {store_id}: {str(e)}")
        return {"status": "error", "store_id": store_id, "error_message": str(e)}

def process_dutchie_pos_data(**kwargs):
    base_url = "https://api.pos.dutchie.com"
    api_keys_table = "dutchiepos-api-keys"
    
    api_keys = get_api_keys(api_keys_table)
    
    endpoints = kwargs['dag_run'].conf.get('endpoints', [])
    parameters = kwargs['dag_run'].conf.get('parameters', {})
    send_to_sqs = kwargs['dag_run'].conf.get("send_to_sqs", False)
    
    if not endpoints:
        raise ValueError('No endpoints provided in the DAG configuration')

    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    results_summary = {
        "total_stores_processed": 0,
        "stores_with_data": 0,
        "stores_with_no_data": 0,
        "errors": [],
    }
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for store_id, api_key_base64 in api_keys.items():
            auth_header = base64.b64encode(f"{api_key_base64}:".encode('utf-8')).decode('utf-8')
            headers = {
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/json"
            }
            
            for endpoint in endpoints:
                api_url = f"{base_url}/{endpoint}"
                params = parameters.get(endpoint, {})
                
                last_processed_date = get_last_processed_date(endpoint)
                
                if 'FromLastModifiedDateUTC' in params:
                    from_date = params['FromLastModifiedDateUTC']
                elif last_processed_date:
                    from_date = last_processed_date
                else:
                    print(f"[WARN] Skipping execution for {endpoint}: No FromLastModifiedDateUTC provided and no record in DynamoDB.")
                    continue
                
                params['FromLastModifiedDateUTC'] = from_date
                params['ToLastModifiedDateUTC'] = current_time

                futures.append(executor.submit(fetch_and_process_data, api_url, endpoint, headers, params, store_id, send_to_sqs))
        
        for future in as_completed(futures):
            result = future.result()
            results_summary["total_stores_processed"] += 1
            if result["status"] == "success":
                results_summary["stores_with_data"] += 1
            elif result["status"] == "no_data":
                results_summary["stores_with_no_data"] += 1
            elif result["status"] == "error":
                results_summary["errors"].append(result)
    
    return results_summary

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dutchie_pos_data_processing',
    default_args=default_args,
    description='A DAG to process Dutchie POS data',
    schedule_interval=timedelta(days=1),
    catchup=False
)

process_data_task = PythonOperator(
    task_id='process_dutchie_pos_data',
    python_callable=process_dutchie_pos_data,
    provide_context=True,
    dag=dag,
)

process_data_task