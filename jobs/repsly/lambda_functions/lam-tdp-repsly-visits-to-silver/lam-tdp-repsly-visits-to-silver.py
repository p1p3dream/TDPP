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

def transform_visits(cursor):
    try:
        # SQL to perform SCD Type 2 transformation
        scd2_query = f"""
        MERGE INTO {TDP_SNOWFLAKE_DB}.SILVER.REPSLY_VISITS target
        USING (
            SELECT 
                VISITID,
                TIMESTAMP,
                TO_TIMESTAMP_TZ(CAST(SUBSTRING(DATE, 7, 13) AS BIGINT) / 1000) AS DATE,
                NULLIF(REPRESENTATIVECODE, '') AS REPRESENTATIVECODE,
                NULLIF(REPRESENTATIVENAME, '') AS REPRESENTATIVENAME, 
                EXPLICITCHECKIN,
                TO_TIMESTAMP_TZ(CAST(SUBSTRING(DATEANDTIMESTART, 7, 13) AS BIGINT) / 1000) AS DATEANDTIMESTART,
                TO_TIMESTAMP_TZ(CAST(SUBSTRING(DATEANDTIMEEND, 7, 13) AS BIGINT) / 1000) AS DATEANDTIMEEND,
                NULLIF(CLIENTCODE, '') AS CLIENTCODE, 
                NULLIF(CLIENTNAME, '') AS CLIENTNAME,
                NULLIF(STREETADDRESS, '') AS STREETADDRESS, 
                NULLIF(ZIP, '') AS ZIP, 
                NULLIF(ZIPEXT, '') AS ZIPEXT, 
                NULLIF(CITY, '') AS CITY,
                NULLIF(STATE, '') AS STATE,
                NULLIF(COUNTRY, '') AS COUNTRY, 
                NULLIF(TERRITORY, '') AS TERRITORY,
                LATITUDESTART,
                LONGITUDESTART,
                LATITUDEEND,
                LONGITUDEEND,
                PRECISIONSTART,
                PRECISIONEND,
                VISITSTATUSBYSCHEDULE,
                VISITENDED,
                HASH_KEY
            FROM {TDP_SNOWFLAKE_DB}.BRONZE.REPSLY_VISITS
        ) source
        ON target.VISITID = source.VISITID AND target.IS_CURRENT = TRUE
        WHEN MATCHED AND target.HASH_KEY != source.HASH_KEY THEN 
            UPDATE SET 
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (
                VISITID, TIMESTAMP, DATE, 
                REPRESENTATIVECODE, REPRESENTATIVENAME, 
                EXPLICITCHECKIN, DATEANDTIMESTART, DATEANDTIMEEND,
                CLIENTCODE, CLIENTNAME, 
                STREETADDRESS, ZIP, ZIPEXT, 
                CITY, STATE, COUNTRY, TERRITORY,
                LATITUDESTART, LONGITUDESTART, 
                LATITUDEEND, LONGITUDEEND,
                PRECISIONSTART, PRECISIONEND,
                VISITSTATUSBYSCHEDULE, VISITENDED,
                HASH_KEY, EFFECTIVE_FROM, IS_CURRENT, VERSION
            )
            VALUES (
                source.VISITID, source.TIMESTAMP, source.DATE,
                source.REPRESENTATIVECODE, source.REPRESENTATIVENAME,
                source.EXPLICITCHECKIN, source.DATEANDTIMESTART, source.DATEANDTIMEEND,
                source.CLIENTCODE, source.CLIENTNAME,
                source.STREETADDRESS, source.ZIP, source.ZIPEXT,
                source.CITY, source.STATE, source.COUNTRY, source.TERRITORY,
                source.LATITUDESTART, source.LONGITUDESTART,
                source.LATITUDEEND, source.LONGITUDEEND,
                source.PRECISIONSTART, source.PRECISIONEND,
                source.VISITSTATUSBYSCHEDULE, source.VISITENDED,
                source.HASH_KEY, CURRENT_TIMESTAMP(), 
                TRUE, 
                COALESCE((
                    SELECT MAX(VERSION) + 1 
                    FROM REPSLY_VISITS 
                    WHERE VISITID = source.VISITID
                ), 1)
            );
        """
        
        cursor.execute(scd2_query)
        rows_affected = cursor.rowcount
        
        return {
            'status': 'success',
            'rows_processed': rows_affected,
            'message': f'Processed {rows_affected} visit records'
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
            result = transform_visits(cursor)
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