
import boto3
import snowflake.connector
import json
import logging
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TDP_SNOWFLAKE_DB = os.environ['TDP_SNOWFLAKE_DB']

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

def create_snowflake_connection(secret: dict):
    """Create Snowflake connection using provided credentials."""
    try:
        return snowflake.connector.connect(
            user=secret['sfUser'],
            password=secret['sfPassword'],
            account=secret['sfaccount'],
            warehouse=secret['sfWarehouse'],
            database=f"{TDP_SNOWFLAKE_DB}",
            schema="SILVER"
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def transform_products(cursor):
    """
    Transform Products data using SCD Type 2 approach.
    
    Key steps:
    1. Identify new and changed records
    2. Close out old versions of changed records
    3. Insert new versions of records
    """
    try:
        # SQL to perform SCD Type 2 transformation
        scd2_query = f"""
        MERGE INTO {TDP_SNOWFLAKE_DB}.SILVER.REPSLY_PRODUCTS target
        USING (
            SELECT
                NULLIF(CODE, '') AS CODE, 
                NULLIF(NAME, '') AS NAME, 
                NULLIF(PRODUCTGROUPCODE, '') AS PRODUCTGROUPCODE,
                NULLIF(PRODUCTGROUPNAME, '') AS PRODUCTGROUPNAME,
                ACTIVE AS IS_ACTIVE,
                NULLIF(TAG, '') AS TAG,
                UNITPRICE,
                NULLIF(EAN, '') AS EAN,
                NULLIF(NOTE, '') AS NOTE, 
                NULLIF(IMAGEURL, '') AS IMAGEURL,
                NULLIF(MASTERPRODUCT, '') AS MASTERPRODUCT, 
                PACKAGING,
                HASH_KEY
            FROM {TDP_SNOWFLAKE_DB}.BRONZE.REPSLY_PRODUCTS
        ) source
        ON target.CODE = source.CODE AND target.IS_CURRENT = TRUE
        WHEN MATCHED AND target.HASH_KEY != source.HASH_KEY THEN
            UPDATE SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                CODE, NAME, PRODUCTGROUPCODE, PRODUCTGROUPNAME,
                IS_ACTIVE, TAG, UNITPRICE, EAN, NOTE, IMAGEURL,
                MASTERPRODUCT, PACKAGING,
                HASH_KEY, EFFECTIVE_FROM, IS_CURRENT, VERSION
            )
            VALUES (
                source.CODE, source.NAME, source.PRODUCTGROUPCODE, source.PRODUCTGROUPNAME,
                source.IS_ACTIVE, source.TAG, source.UNITPRICE, source.EAN, source.NOTE, source.IMAGEURL,
                source.MASTERPRODUCT, source.PACKAGING,
                source.HASH_KEY, CURRENT_TIMESTAMP(),
                TRUE,
                COALESCE((
                    SELECT MAX(VERSION) + 1
                    FROM REPSLY_PRODUCTS
                    WHERE CODE = source.CODE
                ), 1)
            );
        """
        
        cursor.execute(scd2_query)
        rows_affected = cursor.rowcount
        
        return {
            'status': 'success',
            'rows_processed': rows_affected,
            'message': f'Processed {rows_affected} product records'
        }
    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler function."""
    try:
        # Configuration
        region_name = "us-east-1"

        # Get credentials and create connection
        secret = get_secret(region_name)
        conn = create_snowflake_connection(secret)
        
        with conn.cursor() as cursor:
            # Execute transformation
            result = transform_products(cursor)
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