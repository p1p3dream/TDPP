import boto3
import snowflake.connector
import pandas as pd
import json
import logging
import os
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_snowflake_connection():
    glue_client = boto3.client('glue')
    connection_response = glue_client.get_connection(Name='SnowflakeConnection')
    props = connection_response['Connection']['ConnectionProperties']

    user = props.get('USERNAME', '')
    password = props.get('PASSWORD', '')
    jdbc_url = props.get('JDBC_CONNECTION_URL', '')

    url_base = jdbc_url.replace('jdbc:snowflake://', '')
    account_part, params_part = url_base.split('/?')
    account = account_part.split('.')[0]
    params = dict(param.split('=') for param in params_part.split('&'))

    warehouse = params.get('warehouse', '')
    database = params.get('db', '')
    schema = params.get('schema', '')

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    return conn, database, schema

def lambda_handler(event, context):
    task_token = event.get('taskToken')
    sql_command = event.get('sql_command')
    s3_bucket = event.get('s3_bucket') or os.environ.get('S3_BUCKET')
    s3_prefix = event.get('s3_prefix') or os.environ.get('S3_PREFIX', 'snowflake-exports')
    endpoint = event.get('endpoint') or 'data'

    if not sql_command or not isinstance(sql_command, str):
        raise ValueError("SQL command must be provided as a string in the event payload.")

    if not s3_bucket:
        raise ValueError("S3 bucket name must be provided either in the event or environment variables.")

    stepfunctions_client = boto3.client('stepfunctions')
    s3_client = boto3.client('s3')

    try:
        conn, database, schema = get_snowflake_connection()

        # Create Parquet file name like wfs_{endpoint}.parquet
        file_key = f"{s3_prefix}/wfs_{endpoint}.parquet"
        local_file = f"/tmp/wfs_{endpoint}.parquet"

        # Execute SQL and fetch results
        cursor = conn.cursor()
        cursor.execute(sql_command)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        # Save as Parquet
        df = pd.DataFrame(rows, columns=columns)
        df.to_parquet(local_file, index=False)

        s3_client.upload_file(local_file, s3_bucket, file_key)
        logger.info(f"File uploaded to s3://{s3_bucket}/{file_key}")

        table_name = f"wfs_{endpoint}"
        copy_into_sql = f"""
            COPY INTO {schema}.{table_name}
            FROM '@{database}.{schema}.your_stage/{file_key}'
            FILE_FORMAT = (TYPE = 'PARQUET')
        """

        cursor = conn.cursor()
        cursor.execute(copy_into_sql)
        cursor.close()
        logger.info("COPY INTO command executed successfully")

        if task_token:
            stepfunctions_client.send_task_success(
                taskToken=task_token,
                output=json.dumps({"status": "Success", "s3_file": f"s3://{s3_bucket}/{file_key}"})
            )

        return {'status': 'Success', 's3_file': f's3://{s3_bucket}/{file_key}'}

    except Exception as e:
        logger.error(f"Error: {str(e)}")

        if task_token:
            stepfunctions_client.send_task_failure(
                taskToken=task_token,
                error=type(e).__name__,
                cause=str(e)
            )

        raise e
