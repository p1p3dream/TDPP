import boto3
from sqlalchemy import create_engine, text
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve the secret from AWS Secrets Manager
def get_secret():
    secret_name = "DAT_HANA_CLOUD"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-east-1")

    # Get the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])

# Main Lambda handler function
def lambda_handler(event, context):
    # Retrieve the task token and SQL command from the event payload
    task_token = event.get('taskToken')
    sql_command = event.get('sql_command')

    # Log the incoming event
    logger.info(f"Received event: {event}")

    # Ensure the SQL command is provided
    if not sql_command or not isinstance(sql_command, str):
        raise ValueError("SQL command must be provided as a string in the event payload.")

    # Log the SQL command for debugging
    logger.info(f"SQL Command: {sql_command}")

    # Initialize Step Functions client for sending task success/failure
    stepfunctions_client = boto3.client('stepfunctions')

    try:
        # Retrieve database credentials from Secrets Manager
        secret = get_secret()
        user = secret['user']
        password = secret['password']
        host = secret['host']
        port = secret['port']

        # Create the database connection
        connection_url = f"hana://{user}:{password}@{host}:{port}"
        engine = create_engine(connection_url)

        # Execute the SQL command
        with engine.connect() as conn:
            conn.execute(text(sql_command))
            conn.commit()
            logger.info(f"Executed SQL command: {sql_command}")

        # If task token is provided, notify Step Functions of task success
        if task_token:
            stepfunctions_client.send_task_success(
                taskToken=task_token,
                output=json.dumps({"status": "Success"})
            )

    except Exception as e:
        # If task token is provided, notify Step Functions of task failure
        if task_token:
            stepfunctions_client.send_task_failure(
                taskToken=task_token,
                error=str(type(e)),
                cause=str(e)
            )
        # Log or raise exception for monitoring
        logger.error(f"Error executing SQL command: {e}")
        raise e

    return {'status': 'Success'}
