import os
import boto3
import json
import base64
import logging
import re
from botocore.exceptions import ClientError
import snowflake.connector
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend

# Configure logging
logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name="us-east-1"):
    """
    Retrieve a secret from AWS Secrets Manager.
    
    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager
        region_name (str): AWS region where the secret is stored
        
    Returns:
        dict or str: The secret value, parsed as JSON if possible, otherwise as string
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            secret_value = response['SecretString']
            try:
                # Try to parse as JSON
                return json.loads(secret_value)
            except json.JSONDecodeError:
                # Return as string if not valid JSON
                return secret_value
        else:
            # Binary secrets (not common, but handled for completeness)
            decoded_binary = base64.b64decode(response['SecretBinary'])
            return decoded_binary
            
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        raise

def load_snowflake_private_key_der(private_key_pem):
    """
    Load and convert a Snowflake private key from PEM format to DER format.
    
    Args:
        private_key_pem (str): The private key in PEM format
        
    Returns:
        bytes: The private key in DER format for Snowflake
    """
    # Replace escaped newlines with real newlines
    if isinstance(private_key_pem, str):
        pem = private_key_pem.replace("\\n", "\n")
    else:
        pem = private_key_pem
        
    # Load and convert the key
    try:
        key_obj = serialization.load_pem_private_key(
            pem.encode('utf-8') if isinstance(pem, str) else pem,
            password=None,
            backend=default_backend()
        )
        
        der_bytes = key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return der_bytes
    except Exception as e:
        logger.error(f"Error loading Snowflake private key: {e}")
        raise

def snowflake_connect(warehouse="AWS_SERVICE_WAREHOUSE", database=None, schema=None, role=None):
    """
    Establish a connection to Snowflake using credentials from Secrets Manager.
    
    Args:
        warehouse (str): Snowflake warehouse to use
        database (str): Snowflake database to use
        schema (str): Snowflake schema to use
        role (str): Snowflake role to use
        
    Returns:
        snowflake.connector.SnowflakeConnection: A Snowflake connection object
    """
    # Get environment to determine which database to use
    env = os.environ.get('ENVIRONMENT', 'dev').lower()
    if database is None:
        database = "TDP_PROD" if env == 'prod' else "TDP_DEV"
        
    # Get Snowflake credentials from Secrets Manager
    secret_name = "snowflake-private-key"
    
    try:
        # Get secret
        secret = get_secret(secret_name)
        
        # Load private key
        private_key_der = load_snowflake_private_key_der(secret['private_key'])
        
        # Connection parameters
        conn_params = {
            'user': secret['username'],
            'account': secret['account'],
            'private_key': private_key_der,
            'warehouse': warehouse,
        }
        
        # Add optional parameters if provided
        if database:
            conn_params['database'] = database
        if schema:
            conn_params['schema'] = schema
        if role:
            conn_params['role'] = role
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(**conn_params)
        
        return conn
    
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

def verify_sendgrid_signature(signature_b64, timestamp, payload, public_key_pem):
    """
    Verify the signature of a SendGrid webhook event.

    Args:
        signature_b64 (str): The signature of the webhook event
        timestamp (str): The timestamp of the webhook event
        payload (bytes): The payload of the webhook event
        public_key_pem (str): The public key of the SendGrid account in PEM format

    Returns:
        bool: True if the signature is valid, False otherwise
    """
    try:
        # Concatenate timestamp and payload
        signed_data = timestamp.encode('utf-8') + payload

        # Decode signature from base64
        signature = base64.b64decode(signature_b64)

        # Load public key
        public_key = serialization.load_pem_public_key(
            public_key_pem.encode('utf-8') if isinstance(public_key_pem, str) else public_key_pem,
            backend=default_backend()
        )

        # Verify the signature using ECDSA with SHA-256
        public_key.verify(
            signature,
            signed_data,
            ec.ECDSA(hashes.SHA256())
        )

        return True
    except InvalidSignature:
        logger.warning("Invalid SendGrid signature detected")
        return False
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False

def split_sql_statements(sql_content):
    """
    Split SQL content into individual statements.
    
    Args:
        sql_content (str): SQL content with multiple statements
        
    Returns:
        list: List of individual SQL statements
    """
    # This regex pattern looks for semicolons that are not inside quotes or comments
    statements = []
    current_statement = []
    
    # Split by semicolons but preserve them
    parts = sql_content.split(';;')
    
    # Process all parts except the last one (which might be empty)
    for i, part in enumerate(parts):
        current_statement.append(part)
        
        # # If this is not just whitespace, add it as a statement
        if part.strip():
            # Only add semicolon back if this isn't the last part or if the last part isn't empty
            if i < len(parts) - 1 or parts[-1].strip():
                statements.append(';'.join(current_statement) + ';')
                current_statement = []
    
    # If there's anything left in current_statement, add it too
    if current_statement and ''.join(current_statement).strip():
        statements.append(''.join(current_statement))
    
    # Filter out empty statements
    # statements = [stmt for stmt in statements if stmt.strip()]
    statements = parts
    print(statements)
    
    return statements

def execute_sql_file(conn, sql_file_path, database=None, params=None):
    """
    Execute SQL from a file with optional parameter substitution.
    Handles multiple SQL statements in a single file.
    
    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        sql_file_path (str): Path to the SQL file
        database (str): Database name to substitute in the SQL
        params (dict): Parameters to substitute in the SQL
        
    Returns:
        list: Query results (if any)
    """
    # Get environment to determine which database to use if not specified
    if database is None:
        env = os.environ.get('ENVIRONMENT', 'dev').lower()
        database = "TDP_PROD" if env == 'prod' else "TDP_DEV"
    
    # Read the SQL file
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Replace database references
    sql_content = sql_content.replace('TDP_DEV', database)
    sql_content = sql_content.replace('TDP_PROD', database)
    
    # Replace any additional parameters
    if params:
        for key, value in params.items():
            sql_content = sql_content.replace(f"${{{key}}}", str(value))
    
    # Split the SQL content into individual statements
    statements = split_sql_statements(sql_content)
    
    logger.info(f"Found {len(statements)} SQL statements in file {sql_file_path}")
    
    # Execute each statement
    cursor = conn.cursor()
    results = []
    
    try:
        for i, statement in enumerate(statements, 1):
            if statement.strip():  # Skip empty statements
                logger.info(f"Executing statement {i} of {len(statements)} \n {statement}")
                cursor.execute(statement)
                
                # Fetch results if any
                if cursor.description:
                    result = cursor.fetchall()
                    results.append(result)
        
        return results if results else True
    except Exception as e:
        logger.error(f"Error executing SQL statement {i} of {len(statements)}: {e}")
        raise
    finally:
        cursor.close()

def insert_json_to_snowflake(conn, json_data, table_name, column_name="json_content", schema=None):
    """
    Insert JSON data into a Snowflake table.
    
    Args:
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection
        json_data (list or dict): JSON data to insert
        table_name (str): Target table name
        column_name (str): Column name for JSON data
        schema (str): Schema name (uses connection default if None)
        
    Returns:
        int: Number of rows inserted
    """
    cursor = conn.cursor()
    
    try:
        # Determine the schema to use
        schema_prefix = f"{schema}." if schema else ""
        
        # Prepare the SQL
        sql = f"""
            INSERT INTO {schema_prefix}{table_name} ({column_name})
            SELECT PARSE_JSON(%s)
        """
        
        # Convert to list if it's a single object
        if isinstance(json_data, dict):
            json_data = [json_data]
            
        # Insert each JSON object
        row_count = 0
        for item in json_data:
            cursor.execute(sql, (json.dumps(item),))
            row_count += 1
            
        conn.commit()
        return row_count
        
    except Exception as e:
        logger.error(f"Error inserting JSON data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def find_sql_files(job_prefix=None, base_dir='jobs'):
    """
    Find all SQL files in the ddl directories for the specified job or all jobs.
    
    Args:
        job_prefix (str): Optional job prefix to search
        base_dir (str): Base directory where jobs are located
        
    Returns:
        list: Paths to SQL files
    """
    import glob
    import os
    
    # Determine which job directories to look in
    if job_prefix:
        # If job prefix is specified, only look in that job's ddl directory
        job_dirs = [os.path.join(base_dir, job_prefix)]
    else:
        # Otherwise look in all job directories
        job_dirs = glob.glob(os.path.join(base_dir, '*'))
    
    # Find all SQL files in the ddl directories
    sql_files = []
    for job_dir in job_dirs:
        ddl_path = os.path.join(job_dir, 'ddl')
        if os.path.isdir(ddl_path):
            sql_files.extend(glob.glob(f"{ddl_path}/*.sql"))
    
    return sql_files

# Alias for backward compatibility
get_public_key = get_secret
get_snowflake_connection = snowflake_connect

if __name__ == "__main__":
    # This allows the module to be run as a script for testing
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Test code can be added here
    print("DAT Utils module loaded successfully")