import boto3
from botocore.exceptions import ClientError
import re
import os

s3 = boto3.client('s3')
BUCKET = os.environ['FINANCE_BUCKET']
EXCLUDED_PREFIXES = ['00-archive/', '00-report-querys/']
ARCHIVE_PREFIX = '00-archive/'

def lambda_handler(event, context):
    try:
        # List all folders in the bucket
        response = s3.list_objects_v2(Bucket=BUCKET, Delimiter='/')
        
        for prefix in response.get('CommonPrefixes', []):
            folder = prefix.get('Prefix')
            
            # Skip excluded folders
            if any(folder.startswith(excluded) for excluded in EXCLUDED_PREFIXES):
                continue
            
            # Process files in the folder
            process_folder(folder)
        
        return {'statusCode': 200, 'body': 'Process completed successfully'}
    except ClientError as e:
        return {'statusCode': 500, 'body': f'Error during process execution: {e}'}

def process_folder(source_prefix):
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix=source_prefix):
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            
            # Skip the folder itself
            if source_key.endswith('/'):
                continue
            
            # Extract the date from the filename
            match = re.search(r'(\d{4}-\d{2})', source_key)
            if match:
                year_month = match.group(1)  # This will be in the format YYYY-MM
                
                # Create destination key with the new subfolder
                destination_key = f"{ARCHIVE_PREFIX}{source_prefix}{year_month}/{source_key[len(source_prefix):]}"
                
                try:
                    # Copy the object to the new location
                    s3.copy_object(
                        CopySource={'Bucket': BUCKET, 'Key': source_key},
                        Bucket=BUCKET,
                        Key=destination_key
                    )
                    # Only delete the object if the copy was successful
                    s3.delete_object(Bucket=BUCKET, Key=source_key)
                    print(f"Moved {source_key} to {destination_key}")
                except ClientError as copy_error:
                    print(f"Error copying or deleting object {source_key}: {copy_error}")
    
    # Recreate the original empty folder
    try:
        s3.put_object(Bucket=BUCKET, Key=source_prefix)
        print(f"Recreated empty folder: {source_prefix}")
    except ClientError as folder_error:
        print(f"Error recreating empty folder {source_prefix}: {folder_error}")
