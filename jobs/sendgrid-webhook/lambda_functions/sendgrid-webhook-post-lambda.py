import json
import base64
import boto3
import os
from dat_utilities.utils import (
    get_secret,
    verify_sendgrid_signature,
    snowflake_connect,
    insert_json_to_snowflake
)

def lambda_handler(event, context):
    """
    Lambda handler for SendGrid webhook events.
    
    This function:
    1. Validates the SendGrid webhook signature
    2. Connects to Snowflake using credentials from Secrets Manager
    3. Inserts the webhook events into a Snowflake table
    
    Args:
        event (dict): The Lambda event data
        context (object): The Lambda context
        
    Returns:
        dict: Response with status code and message
    """
    # Get environment variables
    env = os.environ.get('ENVIRONMENT', 'dev').lower()
    database = "TDP_PROD" if env == 'prod' else "TDP_DEV"
    schema = "staging"
    table_name = "sendgrid_events_raw_cicd"
    column_name = "json_content"
    
    # 1) Validate the webhook signature
    headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
    signature = headers.get("x-twilio-email-event-webhook-signature")
    timestamp = headers.get("x-twilio-email-event-webhook-timestamp")
    body = event['body']
    
    # Get SendGrid public key from AWS Secrets Manager
    secret_name = "sendgrid-public-key"
    try:
        public_key_pem = get_secret(secret_name)
        if not public_key_pem:
            return {"statusCode": 500, "body": "Failed to retrieve SendGrid public key"}
    except Exception as e:
        return {"statusCode": 500, "body": f"Failed to retrieve SendGrid public key: {str(e)}"}
    
    # Decode body if it's base64-encoded
    if event.get('isBase64Encoded', False):
        body_bytes = base64.b64decode(body)
    else:
        body_bytes = body.encode('utf-8')
    
    # Verify the signature
    is_valid = verify_sendgrid_signature(signature, timestamp, body_bytes, public_key_pem)
    if not is_valid:
        return {"statusCode": 401, "body": "Invalid signature"}
    
    # 2) Parse the webhook payload
    try:
        events = json.loads(body)
        if not isinstance(events, list):
            raise ValueError("Expected JSON array")
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Invalid JSON payload: {e}"})
        }
    
    # 3) Connect to Snowflake and insert data
    try:
        conn = snowflake_connect(database=database, schema=schema)
        row_count = insert_json_to_snowflake(
            conn=conn,
            json_data=events,
            table_name=table_name,
            column_name=column_name,
            schema=schema
        )
        
        return {
            "statusCode": 200,
            "body": f"Inserted {row_count} rows into {database}.{schema}.{table_name}."
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }