import json
import urllib3
import boto3
import base64
from botocore.exceptions import ClientError
from datetime import datetime

# Get environment variables
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']


s3 = boto3.client('s3')
http = urllib3.PoolManager()

BUCKET_NAME = "tdp-bronze"

def lambda_handler(event, context):
    try:
        secret = get_secret()

        api_key = secret.get("api_key")
        api_secret = secret.get("api_secret")

        credentials = f"{api_key}:{api_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        url = "https://api.waitly.com/v1/dashboard/stats"

        headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json',
            'Authorization': f'Basic {encoded_credentials}'
        }

        response = http.request('GET', url, headers=headers)

        if response.status == 200:
            original_data = json.loads(response.data.decode('utf-8'))

            if isinstance(original_data, list) and len(original_data) == 1:
                original_data = original_data[0]

            api_timestamp = original_data.get("timestamp", None)
            if not api_timestamp:
                raise ValueError("No 'timestamp' found in the API response.")

            locations_list = original_data.get("locations", [])

            # Category rename mapping
            rename_mapping = {
                "Adult Use - Pickup": "Pickup",
                "Adult Use - Walk-in": "Walk-in",
                "Curbside": "Pickup"
            }

            for loc in locations_list:
                categories = loc.get("waitTime15MinuteMovingAverage", {}).get("byCategory", [])
                updated_categories = {}

                for category in categories:
                    category_name = category.get("name", "")
                    new_name = rename_mapping.get(category_name, category_name)

                    # Merge duplicate categories
                    if new_name in updated_categories:
                        updated_categories[new_name]["value"] += category["value"]
                        updated_categories[new_name]["sampleSize"] += category["sampleSize"]
                    else:
                        updated_categories[new_name] = {
                            "name": new_name,
                            "value": category["value"],
                            "sampleSize": category["sampleSize"]
                        }

                # Calculate weighted averages for each category
                for category in updated_categories.values():
                    if category["sampleSize"] > 0:
                        category["value"] /= category["sampleSize"]  # Compute the weighted average
                        print(f"Weighted average for {category['name']} = {category['value']}")
                    else:
                        print(f" Division by zero for {category['name']}, skipping weighted average calculation.")
                        category["value"] = 0  # Set value to 0 if sampleSize is 0

                loc["waitTime15MinuteMovingAverage"]["byCategory"] = list(updated_categories.values())

            # Preserve timestamp and save transformed data
            final_data = {
                "timestamp": api_timestamp,
                "locations": locations_list
            }

            current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            transformed_file_name = f"waitly-live-{current_timestamp}.json"

            save_success = save_to_s3(final_data, transformed_file_name)
            if not save_success:
                raise Exception(f"Failed to save transformed data to {transformed_file_name}")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f"Data successfully saved as {transformed_file_name}"
                })
            }
        else:
            return {
                "statusCode": response.status,
                "body": json.dumps({
                    "error": "Failed to fetch data",
                    "details": response.data.decode('utf-8')
                })
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "details": str(e)})
        }

def get_secret():
    secret_name = "waitly-live"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client('secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret

def save_to_s3(data, file_name):
    try:
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=f'waitly-live/{file_name}',
            Body=json.dumps(data),
            ContentType='application/json'
        )
        return True
    except Exception as e:
        print(f"ERROR: Failed to save {file_name} to S3: {str(e)}")
        return False
