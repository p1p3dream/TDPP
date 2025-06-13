import json
import requests
import boto3
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# Initialize clients for S3, DynamoDB, and SQS
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sqs_client = boto3.client('sqs')

# S3 and SQS configurations
BUCKET_NAME = "s3-tru-datalake-ingestion"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/076579646618/sqs-ingestion-dutchiepos-reporting-transactions"
SQS_FAIL_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/076579646618/sqs-ingestion-dutchiepos-raw-failures"

# DynamoDB table names
endpoints_last_processed_table = "dyn-ingestion-raw-dutchiepos-endpoints-last-processed"
transactions_table = "dyn-ingestion-stg-dutchiepos-reporting-transactions"
api_keys_table = "dyn-dutchiepos-stores-api-keys"

def lambda_handler(event, context):
    request_id = context.aws_request_id

    base_url = "https://api.pos.dutchie.com"
    
    # Read endpoints and parameters from the event payload
    endpoints = event.get('endpoints', [])
    clients = event.get('clients', [])
    parameters = event.get('parameters', {})
    send_to_sqs = event.get("send_to_sqs", False)  # Get the flag from the payload
    force_update = event.get("force_update", False) # Get force_update flag

    # Fetch API keys from DynamoDB
    api_keys = get_api_keys(api_keys_table, clients)
    
    if not endpoints:
        return {
            'statusCode': 400,
            'body': json.dumps('No endpoints provided in the event payload')
        }

    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    results_summary = {
        "total_stores_processed": 0,
        "stores_with_data": 0,
        "stores_with_no_data": 0,
        "errors": [],
    }
    
    # Parallel processing with ThreadPoolExecutor
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
                
                # Use only endpoint to retrieve the last processed date
                last_processed_date = get_last_processed_date(endpoint)
                
                if 'FromLastModifiedDateUTC' in params:
                    from_date = params['FromLastModifiedDateUTC']
                elif last_processed_date:
                    from_date = last_processed_date
                else:
                    print_log(request_id, f"[WARN] Skipping execution for {endpoint}: No FromLastModifiedDateUTC provided and no record in DynamoDB.")
                    continue

                if 'ToLastModifiedDateUTC' in params:
                    to_time = params['ToLastModifiedDateUTC']
                else:
                    to_time = current_time
                
                params['FromLastModifiedDateUTC'] = from_date
                params['ToLastModifiedDateUTC'] = to_time

                futures.append(executor.submit(fetch_and_process_data, api_url, endpoint, headers, params, store_id, send_to_sqs, force_update, request_id))
        
        output_payload = []
        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, list):  # This means fetch_and_process_data returned s3_event list
                output_payload.extend(result)
            elif isinstance(result, dict):
                if result["status"] == "success":
                    results_summary["stores_with_data"] += 1
                elif result["status"] == "no_data":
                    results_summary["stores_with_no_data"] += 1
                elif result["status"] == "error":
                    results_summary["errors"].append(result)
            results_summary["total_stores_processed"] += 1

        print(output_payload)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "Data fetched and processed",
            "results_summary": results_summary,
            "output_messages": output_payload
        })
    }

def print_log(request_id, message):
    print(f"[{request_id}] {message}")

def get_api_keys(table_name, clients: list):
    api_keys = {}
    table = dynamodb.Table(table_name)
    if not clients:
        response = table.scan()
        for item in response['Items']:
            api_keys[item['dutchiepos-store-id']] = item['dutchiepos-store-api-key']
    else:
        for client in clients:
            response = table.get_item(Key={
                'dutchiepos-store-id': client,
            })
            if 'Item' in response:
                api_keys[response['Item']['dutchiepos-store-id']] = response['Item']['dutchiepos-store-api-key']
    return api_keys

def get_last_processed_date(endpoint):
    # Fetch last processed date from the appropriate DynamoDB table
    table = dynamodb.Table(endpoints_last_processed_table)
    response = table.get_item(Key={'endpoint': endpoint})
    if 'Item' in response:
        return response['Item'].get('last_runtime', None)
    return None

def save_last_processed_date(endpoint, last_runtime):
    # Fetch last processed date from the appropriate DynamoDB table
    # Update item last_modified value instead of overwriting entire item
    table = dynamodb.Table(endpoints_last_processed_table)
    response = table.get_item(Key={'endpoint': endpoint})
    if 'Item' in response:
        # Save last processed date to the DynamoDB table
        response['Item']['last_runtime'] = last_runtime
        table.put_item(Item=response['Item'])

def fetch_and_process_data(api_url, endpoint, headers, params, store_id, send_to_sqs, force_update, request_id):
    try:
        print_log(request_id, f"[INFO] Requesting data for store ID {store_id} from {api_url}")
        print_log(request_id, f"[INFO] Parameters: {params}")
        
        api_call_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        response = requests.get(api_url, headers=headers, params=params)
        api_response_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        if response.status_code == 200:
            data = response.json()
            if data:
                total_records = len(data)
                print_log(request_id, f"[INFO] Data retrieved for store ID {store_id}: {total_records} records")
                
                # Save the data to S3 only if there is data
                if total_records > 0:
                    # Add metadata to payload
                    metadata = {}
                    metadata['endpoint'] = endpoint
                    metadata['clientid'] = store_id
                    metadata['parameters'] = params
                    metadata['headers'] = {}
                    metadata['calltimeutc'] = api_call_time
                    metadata['responsetimeutc'] = api_response_time
                    metadata['send_to_sqs'] = send_to_sqs
                    metadata['force_update'] = force_update
                    metadata['status'] = response.status_code
                    
                    s3_key = save_data_to_s3(BUCKET_NAME, endpoint, store_id, make_payload(metadata, data), request_id)
                
                    output_messages = []
                    if send_to_sqs:
                        # Create event json
                        s3_event = {
                            'bucket':       BUCKET_NAME,
                            'key':          s3_key,
                            'force_update': force_update,
                            'store_id': store_id,
                            'endpoint': endpoint
                        }
                        output_messages.append(s3_event)

                    # Return list of messages to be handled by Airflow
                    return output_messages
                
                print_log(request_id, f"[INFO] Total records processed for store ID {store_id}: {total_records}")
                
                # Save the last processed date
                save_last_processed_date(endpoint, params['ToLastModifiedDateUTC'])
                
                return {"status": "success", "store_id": store_id, "records_fetched": total_records}
            else:
                print_log(request_id, f"[INFO] No data returned for {endpoint} at store {store_id} with parameters {params}.")
                return {"status": "no_data", "store_id": store_id}
        else:
            print_log(request_id, f"[ERROR] Failed to fetch data for {endpoint} at store {store_id}: {response.status_code} {response.text}")
            fail_event = {
                'request_id': request_id,
                'api_url': api_url,
                'headers': headers,
                'params': params,
                'client_id': store_id,
                'send_to_sqs': send_to_sqs,
                'force_update': force_update,
                'error_message': response.text
            }
            send_fail_to_sqs(SQS_FAIL_QUEUE_URL, fail_event, request_id)
            return {"status": "error", "store_id": store_id, "error_message": response.text}
    except Exception as e:
        print_log(request_id, f"[ERROR] Exception during processing for store ID {store_id}: {str(e)}")
        fail_event = {
            'request_id': request_id,
            'api_url': api_url,
            'headers': headers,
            'params': params,
            'client_id': store_id,
            'send_to_sqs': send_to_sqs,
            'force_update': force_update,
            'error_message': str(e)
        }
        send_fail_to_sqs(SQS_FAIL_QUEUE_URL, fail_event, request_id)
        return {"status": "error", "store_id": store_id, "error_message": str(e)}
        
def make_payload(metadata, response):
    # Shallow copy metadata to create new object to add response to
    payload = dict(metadata)
    payload['response'] = response
    return payload

def save_data_to_s3(bucket_name, endpoint, store_id, data, request_id):
    timestamp = datetime.utcnow().strftime('%Y%m%d.%H%M%S')
    year = datetime.utcnow().strftime('%Y')
    month = datetime.utcnow().strftime('%m')
    day = datetime.utcnow().strftime('%d')
    
    # Modify the folder structure according to the endpoint
    if endpoint == 'reporting/transactions':
        folder_path = 'transactions'
    elif endpoint == 'reporting/customers':
        folder_path = 'customers'
    elif endpoint == 'reporting/products':
        folder_path = 'products'
    # Add more conditions for other endpoints
    
    filename = f"{endpoint.replace('/', '.')}.{store_id}.{timestamp}.json"
    s3_key = f'raw/dutchiepos/{folder_path}/{year}/{month}/{day}/{store_id}/{filename}'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(data))
    print_log(request_id, f'[INFO] Data saved to s3://{bucket_name}/{s3_key}')
    return s3_key

def send_all_to_sqs(queue_url, s3_event, store_id, request_id):
    # Send event to SQS
    try:
        # MessageGroupId is required, can be used for multiple ordered streams but not necessary here
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(s3_event))
        print_log(request_id, f"[INFO] Sent s3 event for {store_id} to SQS queue: {queue_url}")
    except Exception as e: 
        print_log(request_id, f"[ERROR] Failed to send s3 event to SQS: {str(e)}")

def send_fail_to_sqs(queue_url, event, request_id):
    # Send event to SQS
    try:
        # MessageGroupId is required, can be used for multiple ordered streams but not necessary here
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(event))
    except Exception as e: 
        print_log(request_id, f"[ERROR] Failed to send event to failure SQS: {str(e)}")