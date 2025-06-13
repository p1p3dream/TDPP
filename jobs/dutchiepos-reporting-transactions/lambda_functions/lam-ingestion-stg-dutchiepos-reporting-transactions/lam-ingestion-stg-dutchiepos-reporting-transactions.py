import json
import boto3
import hashlib
import traceback
from datetime import datetime

# Initialize clients for DynamoDB and S3
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# S3 bucket and DynamoDB table names
STAGING_BUCKET_NAME = 's3-tru-datalake-ingestion'
DYNAMODB_TABLE_NAME = 'dyn-ingestion-stg-dutchiepos'
DYNAMODB_RECORDKEY_TABLE_NAME = 'dyn-ingestion-raw-dutchiepos-endpoints-last-processed'

# SQS info
queue_url = 'https://sqs.us-east-1.amazonaws.com/076579646618/sqs-ingestion-dutchiepos-bronze'

# Function to check deduplication status in DynamoDB
def check_transaction(endpoint_key, cdc_value, checksum, force_update, request_id):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        # Query the transaction based on the composite key (endpoint_key and lastModifiedDateUTC)
        response = table.get_item(Key={
            'endpoint_key': endpoint_key
        })
        
        if 'Item' in response:
            # Force update if parameter is set to true
            if force_update:
                return 'force_update'

            existing_cdc_value = response['Item']['cdc_field']
            existing_checksum = response['Item']['checksum']
            
            # Compare the cdc_field and checksum
            if cdc_value is not None:
                if cdc_value > existing_cdc_value or checksum != existing_checksum:
                    print_log(request_id, f"[INFO] Hashes matched for record {endpoint_key} or last modified was updated: {checksum} == {existing_checksum}")
                    return 'update'
                else:
                    return 'ignore'
            else:
                if checksum != existing_checksum:
                    print_log(request_id, f"[INFO] Hashes matched for record {endpoint_key}: {checksum} == {existing_checksum}")
                    return 'update'
                else:
                    return 'ignore'
        else:
            return 'new'
    except Exception as e:
        print_log(request_id, f"[ERROR] Error checking DynamoDB: {endpoint_key}")
        print_log(request_id, traceback.format_exc())
        raise Exception(f'[{request_id}] {str(e)}')

# Function to update or insert transaction record into DynamoDB
def update_transaction_record(endpoint_key, cdc_value, checksum, status, request_id):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    try:
        table.put_item(
            Item={
                'endpoint_key': endpoint_key,
                'cdc_field': cdc_value,
                'last_processed': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'checksum' : checksum,
                'status': status  # 'insert' for new transactions, 'update' for updates
            }
        )
        # print(f'Updated dynamo with {endpoint_key}, {cdc_value}, {checksum}, {status}')
    except Exception as e:
        print_log(request_id, f"[ERROR] Error updating DynamoDB: {endpoint_key}")
        raise Exception(f'[{request_id}] {str(e)}')

# Function to write deduplicated data to staging S3
def write_to_curated_s3(endpoint_table, store_id, process_time, data, request_id):
    s3_key = f'curated/dutchiepos/{endpoint_table}/{store_id}/{process_time}.json'
    
    try:
        s3_client.put_object(
            Bucket=STAGING_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data)
        )
        print_log(request_id, f"[INFO] Data written to s3://{STAGING_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print_log(request_id, f"[ERROR] Error writing to S3: {s3_key}")
        raise Exception(f'[{request_id}] {str(e)}')

def send_all_to_sqs(queue_url, s3_event, store_id, request_id):
    # Send event to SQS
    try:
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(s3_event))
        print_log(request_id, f"[INFO] Sent s3 event for {store_id} to SQS queue: {queue_url}")
    except Exception as e: 
        print_log(request_id, f"[ERROR] Failed to send s3 event to SQS: {s3_event}")
        raise Exeption(f'[{request_id}] {str(e)}')

def print_log(request_id, message):
    print(f"[{request_id}] {message}")

# Lambda handler function
def lambda_handler(event, context):
    request_id = context.aws_request_id
    xcom_output = [] 
    print_log(request_id, f"******* Running with event: {event}")

    if isinstance(event, list):
        records = event
    elif 'output_messages' in event:
        records = event['output_messages']
    elif 'body' in event:
        body_data = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        records = body_data.get('output_messages', [])
    else:
        records = []

    for record in records:
        print_log(request_id, f"*************** Processing: {record}")
        try:
            # Get file location from event
            s3_bucket_name = record["bucket"]
            s3_file_name = record["key"]
            store_id = record["store_id"]
            endpoint = record["endpoint"]
            force_update = record.get("force_update", False)

        except Exception as e:
            print_log(request_id, f"[ERROR] Failed to get bucket/file information from event:{record}")
            raise Exception(f'[{request_id}] {str(e)}')

        # Get table name from s3 key
        endpoint_table = s3_file_name.split('/')[2]

        # Decode file into dictionary
        try:
            object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
            body = object['Body']
            json_string = body.read().decode('utf-8')
            payload = json.loads(json_string)
            print_log(request_id, f"[INFO] Decoded and parsed S3 data: {record}")
        except Exception as e:
            print_log(request_id, f"[ERROR] Failed to decode and parse S3 data: {record}")
            raise Exception(f'[{request_id}] {str(e)}')

        # Create payload of new/updated records to insert
        payload_insert = dict(payload)
        payload_insert['response'] = []

        # Keep track of dynamo items to update
        dyn_update = []

        # Get record key and cdc field from dynamo
        store_id = payload.get('clientid', 'unknown')
        table = dynamodb.Table(DYNAMODB_RECORDKEY_TABLE_NAME)
        response = table.get_item(Key={
            'endpoint': payload['endpoint']
        })
        insert = False
        cdc = True
        client_specific = True
        if 'Item' in response:
            if 'record_key' in response['Item']:
                record_key_list = response['Item']['record_key']
            else:
                insert = True

            if 'cdc_field' in response['Item']:
                cdc_field = response['Item']['cdc_field']
            else:
                cdc = False

            # Default to client_specific true
            if 'client_specific' in response['Item']:
                client_specific = response['Item']['client_specific']
            else:
                client_specific = False
        else:
            print_log(request_id, f"[ERROR] Endpoint not found in DynamoDB table {DYNAMODB_RECORDKEY_TABLE_NAME}.")
            continue
        
        for item in payload['response']:
            # Probably want to change this as a param to insert without check rather than doing it by default
            if insert:
                # No primary key specified, so just insert the record
                payload_insert['response'].append(item)
                print_log(request_id, f"[INFO] Record with no primary key inserted.")
            else:
                # Get record key
                if client_specific:
                    record_key = payload['endpoint'] + '_' + payload['clientid'] + '_' + '_'.join([str(item.get(i)) for i in record_key_list])
                else:
                    record_key = payload['endpoint'] + '_' + '_'.join([str(item.get(i)) for i in record_key_list])

                # Check for cdc field
                if cdc:
                    cdc_value = item.get(cdc_field)
                    if not record_key or not cdc_value:
                        print_log(request_id, f"[ERROR] Missing record keys[{','.join(record_key_list)}] or {cdc_field} in the payload.")
                        continue
                else:
                    cdc_value = None

                # Calculate checksum
                checksum = hashlib.sha512(json.dumps(item).encode('utf-8')).hexdigest()
                
                # Check deduplication in DynamoDB
                dedup_status = check_transaction(record_key, cdc_value, checksum, force_update, request_id)
                
                if dedup_status == 'ignore':
                    # Ignore old transactions
                    print_log(request_id, f"[INFO] Transaction {record_key} ignored as it is already up to date.")
                else:
                    dyn_update.append((record_key, cdc_value, checksum, dedup_status))
                    payload_insert['response'].append(item)
                    print_log(request_id, f'[INFO] Processed {record_key} as {dedup_status}')

        # Write final payload to s3 and update dynamo
        process_time = datetime.utcnow()
        payload_insert['last_processed_datetime_utc'] = process_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        if dyn_update:
            # Write data to parquet in s3
            write_to_curated_s3(endpoint_table, store_id, process_time.strftime('%Y%m%d.%H%M%S'), payload_insert, request_id)
            
            # Update transactions in dynamo control table
            for item in dyn_update:
                update_transaction_record(item[0], item[1], item[2], item[3], request_id)

            xcom_output.append({
                "bucket": STAGING_BUCKET_NAME,
                "key": f"curated/dutchiepos/{endpoint_table}/{store_id}/{process_time.strftime('%Y%m%d.%H%M%S')}.json",
                "store_id": store_id,
                "endpoint": endpoint,
                "force_update": force_update
            })


        print_log(request_id, f"[INFO] {len(payload_insert['response'])} records stored in staging: [" + ",".join([i[0] for i in dyn_update]) + "]")

    print_log(request_id, f"[XCOM OUTPUT] {json.dumps(xcom_output)}")
    return { 
        "message": "STG Lambda completed",
        "files_processed": len(xcom_output),
        "output_messages": xcom_output
    }
