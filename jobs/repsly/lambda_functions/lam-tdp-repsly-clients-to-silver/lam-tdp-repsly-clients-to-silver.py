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

def transform_clients(cursor):
    """
    Transform Clients data using SCD Type 2 approach.
    
    Key steps:
    1. Identify new and changed records
    2. Close out old versions of changed records
    3. Insert new versions of records
    """
    try:
        # SQL to perform SCD Type 2 transformation
        scd2_query = f"""
        MERGE INTO {TDP_SNOWFLAKE_DB}.SILVER.REPSLY_CLIENTS target
        USING (
            SELECT 
                CLIENTID,
                TIMESTAMP,
                CODE,
                NULLIF(NAME, '') AS NAME, 
                ACTIVE AS IS_ACTIVE,
                NULLIF(TAG, '') AS TAG,
                NULLIF(TERRITORY, '') AS TERRITORY,
                NULLIF(REPRESENTATIVECODE, '') AS REPRESENTATIVECODE,
                NULLIF(REPRESENTATIVENAME, '') AS REPRESENTATIVENAME,
                NULLIF(STREETADDRESS, '') AS STREETADDRESS,
                NULLIF(ZIP, '') AS ZIP,
                NULLIF(ZIPEXT, '') AS ZIPEXT,
                NULLIF(CITY, '') AS CITY,
                NULLIF(STATE, '') AS STATE,
                NULLIF(COUNTRY, '') AS COUNTRY,
                NULLIF(EMAIL, '') AS EMAIL,
                NULLIF(PHONE, '') AS PHONE,
                NULLIF(MOBILE, '') AS MOBILE,
                NULLIF(WEBSITE, '') AS WEBSITE,
                NULLIF(CONTACTNAME, '') AS CONTACTNAME,
                NULLIF(CONTACTTITLE, '') AS CONTACTTITLE,
                NULLIF(NOTE, '') AS NOTE,
                NULLIF(STATUS, '') AS STATUS,
                PRICELISTS,
                CUSTOMFIELDS,
                NULLIF(GPSLONGITUDE, '') AS GPSLONGITUDE,
                NULLIF(GPSLATITUDE, '') AS GPSLATITUDE,
                NULLIF(ACCOUNTCODE, '') AS ACCOUNTCODE,
                HASH_KEY
            FROM {TDP_SNOWFLAKE_DB}.BRONZE.REPSLY_CLIENTS
        ) source
        ON target.CLIENTID = source.CLIENTID AND target.IS_CURRENT = TRUE
        WHEN MATCHED AND target.HASH_KEY != source.HASH_KEY THEN 
            UPDATE SET 
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (
                CLIENTID, TIMESTAMP, CODE, NAME, IS_ACTIVE, 
                TAG, TERRITORY, REPRESENTATIVECODE, REPRESENTATIVENAME,
                STREETADDRESS, ZIP, ZIPEXT, CITY, STATE, COUNTRY, 
                EMAIL, PHONE, MOBILE, WEBSITE, CONTACTNAME, CONTACTTITLE,
                NOTE, STATUS, PRICELISTS, CUSTOMFIELDS, GPSLONGITUDE, GPSLATITUDE,
                ACCOUNTCODE, HASH_KEY, EFFECTIVE_FROM, IS_CURRENT, VERSION
            )
            VALUES (
                source.CLIENTID, source.TIMESTAMP, source.CODE, source.NAME, source.IS_ACTIVE, 
                source.TAG, source.TERRITORY, source.REPRESENTATIVECODE, source.REPRESENTATIVENAME,
                source.STREETADDRESS, source.ZIP, source.ZIPEXT, source.CITY, source.STATE, source.COUNTRY,
                source.EMAIL, source.PHONE, source.MOBILE, source.WEBSITE, source.CONTACTNAME, source.CONTACTTITLE,
                source.NOTE, source.STATUS, source.PRICELISTS, source.CUSTOMFIELDS, source.GPSLONGITUDE, source.GPSLATITUDE,
                source.ACCOUNTCODE, source.HASH_KEY, CURRENT_TIMESTAMP(), 
                TRUE, 
                COALESCE((SELECT MAX(VERSION) + 1 
                    FROM {TDP_SNOWFLAKE_DB}.SILVER.REPSLY_CLIENTS 
                    WHERE CLIENTID = source.CLIENTID), 1)
            );
        """
        
        cursor.execute(scd2_query)
        rows_affected = cursor.rowcount
        
        return {
            'status': 'success',
            'rows_processed': rows_affected,
            'message': f'Processed {rows_affected} client records'
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
            result = transform_clients(cursor)
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