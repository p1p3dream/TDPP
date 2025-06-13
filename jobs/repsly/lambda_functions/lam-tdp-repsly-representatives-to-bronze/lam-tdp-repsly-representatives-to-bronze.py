import boto3
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from datetime import datetime
import json
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TDP_SNOWFLAKE_DB = os.environ['TDP_SNOWFLAKE_DB']

def get_secret(region_name: str) -> dict:
    """
    Fetch Snowflake credentials from AWS Secrets Manager.
    
    Args:
        region_name (str): AWS region name
    Returns:
        dict: Dictionary containing Snowflake credentials
    """
    secret_name = "sfsecret"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except Exception as e:
        logger.error(f"Error fetching secret: {str(e)}")
        raise

def create_snowflake_connection(secret: dict) -> snowflake.connector.SnowflakeConnection:
    """
    Create Snowflake connection using provided credentials.
    
    Args:
        secret (dict): Dictionary containing Snowflake credentials
    Returns:
        snowflake.connector.SnowflakeConnection: Snowflake connection object
    """
    try:
        return snowflake.connector.connect(
            user=secret['sfUser'],
            password=secret['sfPassword'],
            account=secret['sfaccount'],
            warehouse=secret['sfWarehouse'],
            database=f"{TDP_SNOWFLAKE_DB}",
            schema="BRONZE"
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def check_files_in_stage(cursor: SnowflakeCursor, stage_name: str, subfolder_path: str) -> bool:
    """
    Check if there are any files in the stage to process.
    
    Args:
        cursor: Snowflake cursor
        stage_name (str): Stage name
        subfolder_path (str): Path to the subfolder containing data files
    Returns:
        bool: True if files exist, False otherwise
    """
    try:
        list_query = f"LIST @{stage_name}/{subfolder_path}"
        result = cursor.execute(list_query).fetchall()
        return len(result) > 0
    except Exception as e:
        logger.error(f"Error checking files in stage: {str(e)}")
        raise

def execute_copy_command(cursor: SnowflakeCursor, table_name: str, stage_name: str, subfolder_path: str) -> dict:
    """
    Execute TRUNCATE and COPY INTO command with proper error handling and validation.
    
    Args:
        cursor: Snowflake cursor
        table_name (str): Target table name
        stage_name (str): Stage name
        subfolder_path (str): Path to the subfolder containing data files
    Returns:
        dict: Dictionary with operation results
    """
    try:
        # First, validate the target table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            raise Exception(f"Table {table_name} does not exist")

        # Check if there are files to process
        if not check_files_in_stage(cursor, stage_name, subfolder_path):
            return {
                'status': 'success',
                'message': 'No new files to process',
                'rows_loaded': 0
            }

        # TRUNCATE the table before loading new data
        cursor.execute(f"TRUNCATE TABLE {TDP_SNOWFLAKE_DB}.BRONZE.{table_name}")
        logger.info(f"Table {table_name} truncated successfully")

        # Execute COPY INTO command (same as before)
        copy_query = f"""
        COPY INTO {TDP_SNOWFLAKE_DB}.BRONZE.{table_name}
        (
            CODE,
            NAME,
            NOTE,
            PASSWORD,
            EMAIL,
            PHONE,
            MOBILE,
            TERRITORIES,
            ACTIVE,
            ATTRIBUTES,
            ADDRESS1,
            ADDRESS2,
            CITY,
            STATE,
            ZIPCODE,
            ZIPCODEEXT,
            COUNTRY,
            COUNTRYCODE,
            HASH_KEY,
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            METADATA$FILE_CONTENT_KEY,
            METADATA$FILE_LAST_MODIFIED
        )
        FROM (
            SELECT
                $1:Code::VARCHAR,
                $1:Name::VARCHAR,
                $1:Note::VARCHAR,
                $1:Password::VARCHAR,
                $1:Email::VARCHAR,
                $1:Phone::VARCHAR,
                $1:Mobile::VARCHAR,
                $1:Territories::VARIANT,
                $1:Active::BOOLEAN,
                $1:Attributes::VARIANT,
                $1:Address1::VARCHAR,
                $1:Address2::VARCHAR,
                $1:City::VARCHAR,
                $1:State::VARCHAR,
                $1:ZipCode::VARCHAR,
                $1:ZipCodeExt::VARCHAR,
                $1:Country::VARCHAR,
                $1:CountryCode::VARCHAR,
                $1:Hash_key::VARCHAR,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                METADATA$FILE_CONTENT_KEY,
                METADATA$FILE_LAST_MODIFIED
            FROM @{stage_name}/{subfolder_path}
        )
        FILE_FORMAT = (
            TYPE = 'JSON'
            STRIP_OUTER_ARRAY = TRUE
            IGNORE_UTF8_ERRORS = TRUE
        )
        PATTERN = '.*\\.json$'
        ON_ERROR = CONTINUE;
        """
        
        logger.info("Starting data load...")
        result = cursor.execute(copy_query).fetchall()
        
        # Process results (same logic as before)
        total_rows_loaded = 0
        real_errors = []
        
        for row in result:
            if row and len(row) >= 6:
                file_name = row[0]
                status = row[1]
                rows_in_file = row[2] if row[2] is not None else 0
                errors_seen = row[4]
                first_error = row[5]
                
                total_rows_loaded += rows_in_file
                
                # Only log actual errors
                if errors_seen and errors_seen > 0 and first_error != '0':
                    error_msg = f"Error in file {file_name}: {first_error}"
                    real_errors.append(error_msg)

        # Verify final row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows_after = cursor.fetchone()[0]
        
        # Log result summary
        if real_errors:
            logger.error(f"Load completed with {len(real_errors)} errors. Records loaded: {rows_after}")
            for error in real_errors:
                logger.error(error)
        else:
            logger.info(f"Load completed successfully. Records loaded: {rows_after}")
        
        return {
            'status': 'success' if not real_errors else 'partial_success',
            'message': 'Load completed successfully' if not real_errors else 'Load completed with errors',
            'rows_loaded': rows_after,
            'errors': real_errors
        }
            
    except Exception as e:
        logger.error(f"Error executing COPY command: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler function.
    """
    try:
        # Configuration
        region_name = "us-east-1"
        table_name = "REPSLY_REPRESENTATIVES"
        stage_name = "AWS_S3_STAGE_BRONZE"
        subfolder_path = "repsly/Representatives/"

        # Get credentials and create connection
        secret = get_secret(region_name)
        conn = create_snowflake_connection(secret)
        
        with conn.cursor() as cursor:
            result = execute_copy_command(cursor, table_name, stage_name, subfolder_path)
            conn.commit()
        
        # Return appropriate response
        if result['status'] == 'success' and result['rows_loaded'] == 0:
            return {
                'statusCode': 200,
                'body': json.dumps('No new data to load')
            }
        elif result['status'] in ['success', 'partial_success']:
            response_body = {
                'message': result['message'],
                'rows_loaded': result['rows_loaded']
            }
            # Only include errors if there are actually errors
            if result['errors']:
                response_body['errors'] = result['errors']
                
            return {
                'statusCode': 200,
                'body': json.dumps(response_body)
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error: {result["message"]}')
            }

    except Exception as e:
        logger.error(f"Error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        if 'conn' in locals() and conn:
            conn.close()