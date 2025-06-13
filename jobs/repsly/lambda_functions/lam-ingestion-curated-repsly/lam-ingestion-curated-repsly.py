import json
import boto3
import os
from typing import Dict, List, Optional
from collections import defaultdict

# Get environment variables
REPSLY_DYNAMODB_TABLE = os.environ['REPSLY_DYNAMO_TABLE']
REPSLY_BUCKET = os.environ['REPSLY_BUCKET']
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']

# Configuration for all endpoints
ENDPOINTS = {
    'Clients': {
        'pk':'ClientID',
        'filter_key': 'last_timestamp',
        'source_prefix': 'raw/repsly/Clients/',
        'destination_prefix': 'repsly/Clients/'
    },
    'Products': {
        'pk':'Code',
        'filter_key': 'last_id',
        'source_prefix': 'raw/repsly/Products/',
        'destination_prefix': 'repsly/Products/'
    },
    'Forms': {
        'pk':'ClientID',
        'filter_key': 'last_id',
        'source_prefix': 'raw/repsly/Forms/',
        'destination_prefix': 'repsly/Forms/'
    },
    'Visits': {
        'pk':'VisitID',
        'filter_key': 'last_timestamp',
        'source_prefix': 'raw/repsly/Visits/',
        'destination_prefix': 'repsly/Visits/'
    },
    'Representatives': {
        'pk':'Code',
        'filter_key': None,
        'source_prefix': 'raw/repsly/Representatives/',
        'destination_prefix': 'repsly/Representatives/'
    }
}

def process_json_content(content: str, endpoint: str) -> str:
    """
    Process JSON content by removing MetaCollectionResult and endpoint header
    """
    try:
        data = json.loads(content)
        
        # Remove MetaCollectionResult if present
        if isinstance(data, dict):
            data.pop('MetaCollectionResult', None)
            
            # Remove endpoint header if present
            if endpoint in data:
                endpoint_data = data[endpoint]
                data = endpoint_data
        
        return json.dumps(data, indent=2)
    except Exception as e:
        print(f"Error processing JSON content: {str(e)}")
        raise

def process_s3_copy(
    s3_client: boto3.client,
    source_bucket: str,
    destination_bucket: str,
    source_key: str,
    destination_key: str,
    endpoint: str
) -> bool:
    """
    Process S3 copy operation with error handling and JSON processing
    Adds special handling for Representatives endpoint to clear existing files
    """
    try:
        if endpoint == 'Representatives':
            destination_prefix = ENDPOINTS['Representatives']['destination_prefix']
            
            response = s3_client.list_objects_v2(
                Bucket=destination_bucket,
                Prefix=destination_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [
                    {'Key': obj['Key']} for obj in response['Contents']
                ]
                
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    s3_client.delete_objects(
                        Bucket=destination_bucket,
                        Delete={'Objects': batch}
                    )
                print(f"Deleted {len(objects_to_delete)} existing files in {destination_prefix}")

        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        content = response['Body'].read().decode('utf-8')
        
        modified_content = process_json_content(content, endpoint)
        
        s3_client.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=modified_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        return True
    except Exception as e:
        print(f"Error processing and copying S3 object: {str(e)}")
        return False

def get_uncurated_items(table: boto3.resource, endpoint: Optional[str] = None) -> List[Dict]:
    """
    Get uncurated items from DynamoDB using Query instead of Scan when possible
    """
    try:
        if endpoint:
            # Use Query operation since we know the partition key (endpoint)
            key_condition = 'endpoint = :endpoint_val'
            expression_values = {
                ':endpoint_val': endpoint,
                ':flag_val': False
            }
            
            response = table.query(
                KeyConditionExpression=key_condition,
                FilterExpression='flag_curated = :flag_val',
                ExpressionAttributeValues=expression_values
            )
        else:
            # If no endpoint specified, we need to scan with filter
            response = table.scan(
                FilterExpression='flag_curated = :flag_val',
                ExpressionAttributeValues={':flag_val': False}
            )
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            if endpoint:
                response = table.query(
                    KeyConditionExpression=key_condition,
                    FilterExpression='flag_curated = :flag_val',
                    ExpressionAttributeValues=expression_values,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            else:
                response = table.scan(
                    FilterExpression='flag_curated = :flag_val',
                    ExpressionAttributeValues={':flag_val': False},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            items.extend(response.get('Items', []))
        
        print(f"Found {len(items)} uncurated items")
        return items
        
    except Exception as e:
        print(f"Error getting uncurated items: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return []

def get_all_items(table: boto3.resource, endpoint: Optional[str] = None) -> List[Dict]:
    """
    Get all items from DynamoDB using Query instead of Scan when possible
    """
    try:
        if endpoint:
            # Use Query when we have the partition key
            response = table.query(
                KeyConditionExpression='endpoint = :endpoint_val',
                ExpressionAttributeValues={':endpoint_val': endpoint}
            )
        else:
            # Fall back to Scan when no partition key is provided
            response = table.scan()
        
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            if endpoint:
                response = table.query(
                    KeyConditionExpression='endpoint = :endpoint_val',
                    ExpressionAttributeValues={':endpoint_val': endpoint},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            else:
                response = table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            items.extend(response.get('Items', []))
            
        return items
    except Exception as e:
        print(f"Error getting all items: {str(e)}")
        return []

def update_dynamo_record(
    table: boto3.resource,
    item_id: str,
    s3_path: str,
    endpoint: str,
    processed_timestamp: str
) -> bool:
    """
    Update DynamoDB record using the correct composite key structure
    """
    try:
        print(f"Attempting to update record with id: {item_id}")
        print(f"Setting curated_s3_path to: {s3_path}")
        
        update_response = table.update_item(
            Key={
                'endpoint': endpoint,
                'processed_timestamp': processed_timestamp
            },
            UpdateExpression='SET flag_curated = :flag_val, curated_s3_path = :path_val',
            ExpressionAttributeValues={
                ':flag_val': True,
                ':path_val': s3_path
            },
            ReturnValues='UPDATED_NEW'
        )
        
        print(f"Update response: {json.dumps(update_response, default=str)}")
        return True
            
    except Exception as e:
        print(f"Error updating DynamoDB record for id {item_id}: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return False

def mark_duplicates_in_table(table: boto3.resource, items: List[Dict]) -> None:
    """
    Identifies duplicate hashes in items and updates DynamoDB to mark duplicates
    """
    if not items:
        return
        
    # Get all items for the same endpoint
    endpoint = items[0].get('endpoint')
    all_items = get_all_items(table, endpoint)
    
    # Create a dictionary to store hash lists and their associated IDs
    hash_to_ids = defaultdict(list)
    for item in all_items:
        hashes = item['hash_key'].split(',')
        for hash_value in hashes:
            hash_to_ids[hash_value.strip()].append(item['id'])
    
    # Mark the duplicate hashes in DynamoDB
    for item in items:
        current_id = item['id']
        endpoint = item['endpoint']
        processed_timestamp = item['processed_timestamp']
        hashes = set(hash_value.strip() for hash_value in item['hash_key'].split(','))
        duplicate_info = []
        
        # Group duplicates by their lowest ID
        id_to_hashes = defaultdict(set)
        
        for hash_value in hashes:
            hash_ids = hash_to_ids[hash_value]
            if len(hash_ids) > 1:
                lower_ids = [id for id in hash_ids if id < current_id]
                if lower_ids:
                    min_id = min(lower_ids)
                    id_to_hashes[min_id].add(hash_value)
        
        if id_to_hashes:
            duplicate_info = [
                {
                    'id': min_id,
                    'hash_list': sorted(list(dup_hashes))
                }
                for min_id, dup_hashes in sorted(id_to_hashes.items())
            ]
            
            try:
                table.update_item(
                    Key={
                        'endpoint': endpoint,
                        'processed_timestamp': processed_timestamp
                    },
                    UpdateExpression='SET dup_hash = :val',
                    ExpressionAttributeValues={':val': json.dumps(duplicate_info)}
                )
            except Exception as e:
                print(f"Error updating duplicate info for ID {current_id}: {str(e)}")

def process_items(
    items: List[Dict],
    table: boto3.resource,
    s3_client: boto3.client,
    source_bucket: str,
    destination_bucket: str
) -> Dict[str, int]:
    """
    Process items and return statistics
    """
    stats = {
        'processed': 0,
        'failed': 0,
        'skipped': 0
    }
    
    for item in items:
        try:
            endpoint = item.get('endpoint')
            processed_timestamp = item.get('processed_timestamp')
            
            if endpoint not in ENDPOINTS:
                print(f"Skipping unknown endpoint: {endpoint}")
                stats['skipped'] += 1
                continue
                
            file_name = item.get('filename', '')
            source_key = os.path.join(ENDPOINTS[endpoint]['source_prefix'], file_name)
            destination_key = os.path.join(ENDPOINTS[endpoint]['destination_prefix'], file_name)
            
            print(f"Processing file {file_name} for endpoint {endpoint}")
            print(f"Source key: {source_key}")
            print(f"Destination key: {destination_key}")
            
            if not process_s3_copy(
                s3_client, 
                source_bucket, 
                destination_bucket, 
                source_key, 
                destination_key, 
                endpoint
            ):
                print(f"Failed to copy S3 object for {file_name}")
                stats['failed'] += 1
                continue
                
            if update_dynamo_record(
                table, 
                item['id'], 
                destination_key, 
                endpoint,
                processed_timestamp
            ):
                stats['processed'] += 1
                print(f"Successfully processed {endpoint} file: {file_name}")
            else:
                stats['failed'] += 1
                print(f"Failed to update DynamoDB record for {file_name}")
                
        except Exception as e:
            print(f"Error processing item: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            stats['failed'] += 1
            
    return stats

def lambda_handler(event, context):
    """
    Main Lambda handler for processing Repsly data curation
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('dyn-ingestion-repsly')
        s3_client = boto3.client('s3')
        source_bucket = REPSLY_BUCKET
        destination_bucket = BRONZE_BUCKET
        
        endpoint = event.get('endpoint')
        
        items = get_uncurated_items(table, endpoint)
        
        if not items:
            return {
                'statusCode': 200,
                'body': json.dumps('No uncurated items found to process.')
            }
            
        mark_duplicates_in_table(table, items)
        
        stats = process_items(items, table, s3_client, source_bucket, destination_bucket)
        
        message = (
            f"Processing completed. "
            f"Processed: {stats['processed']}, "
            f"Failed: {stats['failed']}, "
            f"Skipped: {stats['skipped']}"
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': message,
                'stats': stats
            })
        }
        
    except Exception as e:
        error_message = f"Error in lambda execution: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }