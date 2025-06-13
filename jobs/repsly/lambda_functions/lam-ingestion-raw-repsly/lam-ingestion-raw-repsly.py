import json
import boto3
import urllib.request
import base64
import hashlib
import os
from datetime import datetime
import uuid

# API endpoints configuration with their specific attributes
ENDPOINTS = {
    'Clients': {
        'url': 'https://api.repsly.com/v3/export/clients/',
        'filter': 'last_timestamp',
        'value_key': 'LastTimeStamp',
        'pk': 'ClientID'
    },
    'Products': {
        'url': 'https://api.repsly.com/v3/export/products/',
        'filter': 'last_id',
        'value_key': 'LastID',
        'pk': 'Code'
    },
    'Forms': {
        'url': 'https://api.repsly.com/v3/export/forms/',
        'filter': 'last_id',
        'value_key': 'LastID',
        'pk': 'FormID'
    },
    'Visits': {
        'url': 'https://api.repsly.com/v3/export/visits/',
        'filter': 'last_timestamp',
        'value_key': 'LastTimeStamp',
        'pk': 'VisitID'
    },
    'Representatives': {
        'url': 'https://api.repsly.com/v3/export/representatives',
        'filter': None,
        'value_key': None,
        'pk': 'Code'
    }
}

def get_api_credentials():
    """Retrieve API credentials from AWS Secrets Manager"""
    secret_name = "repsly-api-credentials"
    region_name = "us-east-1" 
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        
        secret = json.loads(response['SecretString'])
        api_username = secret['API_USERNAME']
        api_password = secret['API_PASSWORD']
        
        return api_username, api_password
    
    except Exception as e:
        print(f"Error al obtener el secreto: {e}")
        raise


# API authentication credentials
API_USERNAME, API_PASSWORD = get_api_credentials()

def generate_hash(item):
    """
    Generate a hash for a dictionary item
    Sorts keys to ensure consistent hashing regardless of key order
    """
    # Convert the item to a sorted, normalized string representation
    sorted_item = dict(sorted(item.items()))
    
    # Convert to string, ensuring consistent formatting
    item_str = json.dumps(sorted_item, sort_keys=True)
    
    # Generate SHA-256 hash
    hash_object = hashlib.sha256(item_str.encode())
    return hash_object.hexdigest()

def add_hash_to_items(data, endpoint):
    """
    Add hash to each item in the data
    Returns the modified data and a list of hashes
    """
    items = data.get(endpoint, [])
    item_hashes = []
    
    for item in items:
        item_hash = generate_hash(item)
        item['Hash_key'] = item_hash
        item_hashes.append(item_hash)
    
    return data, item_hashes

def get_last_processed_value(table, endpoint):
    """
    Get the last processed value for a specific endpoint using the table's partition key and sort key
    """
    try:
        filter_name = ENDPOINTS[endpoint]['filter']
        if not filter_name:
            return None
            
        response = table.query(
            KeyConditionExpression='endpoint = :endpoint',
            ExpressionAttributeValues={
                ':endpoint': endpoint
            },
            ProjectionExpression=filter_name,
            ScanIndexForward=False,  # This will sort in descending order (newest first)
            Limit=1
        )
        
        items = response.get('Items', [])
        if not items:
            return 0
            
        # Get the filter value from the most recent record
        last_value = int(items[0].get(filter_name, 0))
        return last_value
        
    except Exception as e:
        print(f"Error getting last processed value: {str(e)}")
        raise

def fetch_api_data(url, endpoint):
    """
    Fetch data from the API with improved empty response handling
    Returns:
    - (data, False) if data is successfully retrieved
    - (None, True) if no more data is available (empty response)
    - (None, False) if there's an error
    """
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    credentials = base64.b64encode(f"{API_USERNAME}:{API_PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {credentials}")
    
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            
            # Handle empty response cases
            if not data:
                print(f"Empty response received for {endpoint}")
                return None, True
                
            # For paginated endpoints
            if endpoint != 'Representatives':
                if 'MetaCollectionResult' not in data:
                    print(f"No MetaCollectionResult in response for {endpoint}")
                    return None, True
                    
                meta_result = data['MetaCollectionResult']
                if meta_result.get('TotalCount', 0) == 0:
                    print(f"No more data available for {endpoint}")
                    return None, True
            
            return data, False
            
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"No more data available for {endpoint} (404 response)")
            return None, True
        print(f"HTTP Error for {endpoint}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Invalid JSON response for {endpoint}: {str(e)}")
        return None, True
    except Exception as e:
        print(f"Error fetching data for {endpoint}: {str(e)}")
        raise

def save_to_s3_and_dynamo(data, endpoint, record_count, table, s3_client, last_value=None):
    """Save data to S3 and DynamoDB with improved error handling and hash functionality"""
    try:
        # Add hash to each item in the data and get list of hashes
        modified_data, item_hashes = add_hash_to_items(data, endpoint)
        
        current_date = datetime.now().strftime('%Y%m%d')
        identifier = current_date if endpoint == 'Representatives' else last_value
        file_name = f"{endpoint}-{identifier}.json"
        s3_path = f"raw/repsly/{endpoint}/{file_name}"
        
        # Save modified data (with hashes) to S3
        s3_client.put_object(
            # Bucket="s3-tru-datalake-ingestion",
            Bucket=os.environ['S3_BUCKET'],
            Key=s3_path,
            Body=json.dumps(modified_data),
            ContentType='application/json'
        )
        
        # Create a unique ID using UUID
        unique_id = str(uuid.uuid4())

        # Create comma-separated string of hashes
        hash_list = ",".join(item_hashes)

        # Create the key_list with primary keys
        primary_keys = [str(item[ENDPOINTS[endpoint]['pk']]) for item in modified_data[endpoint]]
        key_list = ",".join(primary_keys)
        
        # Prepare DynamoDB item
        item = {
            'id': unique_id,  # Use UUID instead of incremental ID
            'endpoint': endpoint,  # Use as partition key
            'processed_timestamp': datetime.now().isoformat(),  # Use as sort key
            'record_count': record_count,
            'flag_curated': False,
            'filename': file_name,
            'executed_by': 'lam-ingestion-raw-repsly',
            's3_path': s3_path,
            'key_list': key_list,
            'hash_key': hash_list
        }
        
        if last_value is not None and ENDPOINTS[endpoint]['filter']:
            item[ENDPOINTS[endpoint]['filter']] = str(last_value)
            
        table.put_item(Item=item)
        print(f"Successfully processed {record_count} records for {endpoint}")
        
    except Exception as e:
        print(f"Error in save operation: {str(e)}")
        raise

def get_last_processed_count(table, endpoint):
    """Get the last processed record count for Representatives"""
    try:
        response = table.scan(
            ProjectionExpression='record_count,processed_date',
            FilterExpression='endpoint = :endpoint',
            ExpressionAttributeValues={':endpoint': endpoint}
        )
        
        items = response.get('Items', [])
        if not items:
            return None
            
        # Get the most recent record based on processed_date
        sorted_items = sorted(items, key=lambda x: x.get('processed_date', ''), reverse=True)
        return int(sorted_items[0].get('record_count', 0))
        
    except Exception as e:
        print(f"Error getting last processed count: {str(e)}")
        raise

def process_representatives(table, s3_client):
    """Process representatives data with hash functionality"""
    try:
        data, is_empty = fetch_api_data(ENDPOINTS['Representatives']['url'], 'Representatives')
        if is_empty:
            print("No Representatives data available")
            return
            
        representatives = data.get('Representatives', []) if not isinstance(data, list) else data
        current_count = len(representatives)
        
        if current_count == 0:
            print("No Representatives data found")
            return
            
        last_count = get_last_processed_count(table, 'Representatives')
        
        if last_count is not None:
            if current_count == last_count:
                print(f"No changes in Representatives count (current: {current_count}, last: {last_count}). Skipping processing.")
                return
            print(f"Changes detected in Representatives count - Previous: {last_count}, Current: {current_count}")
        else:
            print("First time processing Representatives")
            
        formatted_data = {
            'Representatives': representatives,
            'TotalCount': current_count,
            'FetchDate': datetime.now().isoformat()
        }
        
        if last_count is not None:
            formatted_data['PreviousCount'] = last_count
            
        save_to_s3_and_dynamo(formatted_data, 'Representatives', current_count, table, s3_client)
        print(f"Successfully processed Representatives data with count: {current_count}")
            
    except Exception as e:
        print(f"Error processing Representatives: {str(e)}")
        raise

def process_paginated_endpoint(endpoint, table, s3_client):
    """Process paginated endpoints with improved empty response handling"""
    try:
        # Get the last processed value for this endpoint
        last_value = get_last_processed_value(table, endpoint)
        records_processed = False
        
        while True:
            url = f"{ENDPOINTS[endpoint]['url']}{last_value}"
            data, is_empty = fetch_api_data(url, endpoint)
            
            # If we get an empty response, break the loop
            if is_empty:
                break
                
            meta_result = data['MetaCollectionResult']
            count = meta_result.get('TotalCount', 0)
            new_last_value = int(meta_result.get(ENDPOINTS[endpoint]['value_key'], 0))
            
            # Only process if we have new data
            if new_last_value > last_value:
                save_to_s3_and_dynamo(data, endpoint, count, table, s3_client, new_last_value)
                records_processed = True
                last_value = new_last_value
            else:
                break
        
        if not records_processed:
            print(f"No new records to process for {endpoint}")
            
    except Exception as e:
        print(f"Error processing paginated endpoint {endpoint}: {str(e)}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler with improved error handling and validation"""
    endpoint = event.get('endpoint')
    if endpoint not in ENDPOINTS:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid endpoint. Valid endpoints are: {", ".join(ENDPOINTS.keys())}')
        }

    try:
        dynamodb = boto3.resource('dynamodb')
        # table = dynamodb.Table('dyn-ingestion-repsly')
        table = dynamodb.Table(os.environ['REPSLY_DYNAMODB_TABLE'])
        s3_client = boto3.client('s3')
        
        if endpoint == 'Representatives':
            process_representatives(table, s3_client)
        else:
            process_paginated_endpoint(endpoint, table, s3_client)
            
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {endpoint}')
        }
        
    except Exception as e:
        error_message = f"Error processing {endpoint}: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }
