import boto3

s3 = boto3.client('s3')

BUCKET_NAME = 'hr-wfs-bronze'
ARCHIVE_FOLDER = 'archive'

def lambda_handler(event, context):
    # Get folder name from the event
    folder_name = event.get('folder_name')
    
    if not folder_name:
        return {
            'statusCode': 400,
            'body': 'folder_name is required in the event input.'
        }

    # Prefix the folder with 'deltas/' to target the deltas folder
    source_folder = f"deltas/{folder_name}"

    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=source_folder + '/')
    
    if 'Contents' not in response:
        print(f"No files found in {source_folder}")
        return {
            'statusCode': 200,
            'body': 'No files to archive.'
        }
    
    for item in response['Contents']:
        file_key = item['Key']
        
        if not file_key.endswith('/'):  # Exclude directory paths
            file_name = file_key.split('/')[-1]
            archive_key = f"{ARCHIVE_FOLDER}/{folder_name}/{file_name}"
            
            # Copy file to the archive folder
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': file_key},
                Key=archive_key
            )
            
            # Delete the original file
            s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
    
    return {
        'statusCode': 200,
        'body': 'Files archived successfully.'
    }
