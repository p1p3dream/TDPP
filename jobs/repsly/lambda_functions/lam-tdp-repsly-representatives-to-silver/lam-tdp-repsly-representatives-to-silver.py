import boto3
import snowflake.connector
import json
import logging
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(region_name: str) -> dict:
    """Fetch Snowflake credentials from AWS Secrets Manager."""
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

def create_snowflake_connection(secret: dict, database: str):
    """Create Snowflake connection using provided credentials."""
    try:
        return snowflake.connector.connect(
            user=secret['sfUser'],
            password=secret['sfPassword'],
            account=secret['sfaccount'],
            warehouse=secret['sfWarehouse'],
            database=database,
            schema="SILVER"
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def transform_representatives(cursor, database: str):
    """
    Transform Representatives data using SCD Type 2 approach.
    
    Key steps:
    1. Identify new and changed records
    2. Close out old versions of changed records
    3. Insert new versions of records
    """
    try:
        # SQL to perform SCD Type 2 transformation
        scd2_query = f"""
        MERGE INTO {database}.SILVER.REPSLY_REPRESENTATIVES target
        USING (
            SELECT 
                CODE,
                NULLIF(NAME, '') AS NAME,
                NULLIF(NOTE, '') AS NOTE,
                NULLIF(PASSWORD, '') AS PASSWORD,
                NULLIF(EMAIL, '') AS EMAIL,
                NULLIF(PHONE, '') AS PHONE,
                NULLIF(MOBILE, '') AS MOBILE,
                ACTIVE AS IS_ACTIVE,
                ATTRIBUTES,
                NULLIF(ADDRESS1, '') AS ADDRESS1,
                NULLIF(ADDRESS2, '') AS ADDRESS2,
                NULLIF(CITY, '') AS CITY,
                NULLIF(STATE, '') AS STATE,
                NULLIF(ZIPCODE, '') AS ZIPCODE,
                NULLIF(ZIPCODEEXT, '') AS ZIPCODEEXT,
                NULLIF(COUNTRY, '') AS COUNTRY,
                NULLIF(COUNTRYCODE, '') AS COUNTRYCODE,
                TERRITORIES,
                HASH_KEY
            FROM {database}.BRONZE.REPSLY_REPRESENTATIVES
        ) source
        ON target.CODE = source.CODE AND target.IS_CURRENT = TRUE
        WHEN MATCHED AND target.HASH_KEY != source.HASH_KEY THEN 
            UPDATE SET 
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (
                CODE, NAME, NOTE, PASSWORD, EMAIL, PHONE, MOBILE, 
                IS_ACTIVE, ATTRIBUTES, ADDRESS1, ADDRESS2, 
                CITY, STATE, ZIPCODE, ZIPCODEEXT, COUNTRY, COUNTRYCODE, TERRITORIES,
                HASH_KEY, EFFECTIVE_FROM, IS_CURRENT, VERSION
            )
            VALUES (
                source.CODE, source.NAME, source.NOTE, source.PASSWORD, source.EMAIL, 
                source.PHONE, source.MOBILE, source.IS_ACTIVE, source.ATTRIBUTES,
                source.ADDRESS1, source.ADDRESS2, 
                source.CITY, source.STATE, 
                source.ZIPCODE, source.ZIPCODEEXT, source.COUNTRY, source.COUNTRYCODE,
                source.TERRITORIES, source.HASH_KEY, CURRENT_TIMESTAMP(), 
                TRUE, 
                COALESCE((
                    SELECT MAX(VERSION) + 1 
                    FROM {database}.SILVER.REPSLY_REPRESENTATIVES 
                    WHERE CODE = source.CODE
                ), 1)
            )
        """
        
        cursor.execute(scd2_query)
        rows_affected = cursor.rowcount
        
        return {
            'status': 'success',
            'rows_processed': rows_affected,
            'message': f'Processed {rows_affected} representative records'
        }
    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler function."""
    try:
        # Configuration
        region_name = "us-east-1"
        database = os.environ['TDP_SNOWFLAKE_DB']

        # Get credentials and create connection
        secret = get_secret(region_name)
        conn = create_snowflake_connection(secret, database)
        
        with conn.cursor() as cursor:
            # Execute transformation
            result = transform_representatives(cursor, database)
            conn.commit()
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }
    finally:
        if 'conn' in locals() and conn:
            conn.close()