import json
import requests
import time
import boto3
import logging
import sys
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS clients
s3_resource = boto3.resource('s3')
stepfunctions = boto3.client('stepfunctions')

# Constants
BUCKET_NAME = 'hr-wfs-bronze'
TOKEN_URL = "https://cas-us8.wfs.cloud/aug/api/v1.0/authenticate/client/trulieve?client_id=trulieve_rest_api&client_secret=c244bef3-326b-47ff-9842-5ca25aad4ad8"

# Global variable for caching the token
cached_token = None
token_expiration_time = None

# Configure session with retry and keep-alive
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def main():
    # Read Glue job arguments
    args = sys.argv[1:]
    arguments = {args[i].lstrip("--"): args[i + 1] for i in range(0, len(args), 2)}

    endpoint = arguments.get('endpoint')
    task_token = arguments.get('taskToken')
    use_cursor = arguments.get('use_cursor', 'true').lower() == 'true'

    if not endpoint:
        logger.error("Endpoint not specified")
        sys.exit(1)

    endpoint_url = f'https://api-us8.wfs.cloud/{endpoint}/v1'
    output_filename_cursors = f'cursors/{endpoint.replace("/", "-")}.txt'
    endpoint_params = {"count": 10000}

    try:
        token = get_cached_token()
        endpoint_headers = {"Authorization": f"Bearer {token}"}

        # Delete archive directory if use_cursor is False
        if not use_cursor:
            delete_s3_objects_in_directory(BUCKET_NAME, f'archive/{endpoint}/')
            delete_s3_objects_in_directory(BUCKET_NAME, f'deltas/{endpoint}/')

        result_message = fetch_and_store_data(endpoint_url, endpoint_headers, endpoint_params, output_filename_cursors, endpoint, use_cursor)

        if task_token:
            send_task_success(task_token, {'result': result_message})
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if task_token:
            send_task_failure(task_token, {'error': str(e)})
        sys.exit(1)

def fetch_and_store_data(endpoint_url, headers, params, cursor_filename, endpoint, use_cursor):
    cursor = None
    if use_cursor:
        cursor = get_s3_object_content(BUCKET_NAME, cursor_filename)

    if cursor:
        params["cursor"] = cursor

    last_record_id = None
    token_expiration_buffer = 60  # Buffer time to refresh token before it actually expires

    while True:
        try:
            # Refresh token if it's close to expiring
            global token_expiration_time
            if not token_expiration_time or (time.time() + token_expiration_buffer) > token_expiration_time:
                token = get_cached_token()
                headers["Authorization"] = f"Bearer {token}"

            logger.info(f"Fetching data with cursor: {cursor}")
            
            # Increase timeout to 120 seconds
            response = session.get(endpoint_url, headers=headers, params=params, timeout=120)
            
            logger.info(f"API Response Status Code: {response.status_code}")
            response.raise_for_status()
            data = response.json()

            update_sequence = data.get('updateSequence', [])
            if update_sequence:
                last_record_id = update_sequence[-1].get('id')
                logger.info(f"Retrieved {len(update_sequence)} records")

            save_json_to_s3(data, endpoint)
        except requests.RequestException as e:
            logger.error(f"Error fetching data from {endpoint_url}: {e}")
            raise Exception(f"API request failed for {endpoint_url}")

        next_cursor = data.get("nextCursor")
        if not next_cursor or next_cursor == cursor:
            logger.info("No further pages to fetch, or cursor has not advanced.")
            break

        cursor = next_cursor
        params["cursor"] = cursor

    if use_cursor and cursor and cursor != get_s3_object_content(BUCKET_NAME, cursor_filename):
        put_s3_object(BUCKET_NAME, cursor_filename, cursor)

    return "Data retrieved and stored successfully!"

def get_cached_token():
    global cached_token, token_expiration_time
    if cached_token and token_expiration_time and time.time() < token_expiration_time:
        return cached_token

    token_data = request_wfs_token()
    expires_in = int(token_data.get("expires_in", 600))  
    cached_token = token_data.get("access_token")
    token_expiration_time = time.time() + expires_in - 60  
    return cached_token

def request_wfs_token():
    retries = 3
    for attempt in range(retries):
        try:
            logger.info("Fetching token from WFS API")
            response = session.post(TOKEN_URL, json={"access_token": "string", "expires_in": "integer"}, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching token: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt + (attempt * 0.1))
            else:
                raise Exception("Failed to fetch token after retries")

def save_json_to_s3(data, endpoint):
    update_sequence = data.get('updateSequence', [])
    if not update_sequence:
        logger.info(f"No data in updateSequence for {endpoint}, skipping file write.")
        return

    ndjson_data = '\n'.join(json.dumps(record) for record in update_sequence)
    timestamp = time.strftime("%Y%m%d%H%M%S")
    output_filename = f'deltas/{endpoint.replace("/", "-")}/{endpoint.replace("/", "-")}response.{timestamp}.json'
    put_s3_object(BUCKET_NAME, output_filename, ndjson_data)

def get_s3_object_content(bucket_name, key):
    try:
        obj = s3_resource.Object(bucket_name, key)
        return obj.get()['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.info(f"Key {key} not found in bucket {bucket_name}")
            return None
        else:
            raise

def put_s3_object(bucket_name, key, data):
    s3_resource.Object(bucket_name, key).put(Body=data)

def delete_s3_objects_in_directory(bucket_name, prefix):
    bucket = s3_resource.Bucket(bucket_name)
    objects_to_delete = bucket.objects.filter(Prefix=prefix)
    for obj in objects_to_delete:
        obj.delete()

def send_task_success(task_token, output):
    stepfunctions.send_task_success(
        taskToken=task_token,
        output=json.dumps(output)
    )

def send_task_failure(task_token, error):
    stepfunctions.send_task_failure(
        taskToken=task_token,
        error='GlueJobError',
        cause=json.dumps(error)
    )

if __name__ == "__main__":
    main()
