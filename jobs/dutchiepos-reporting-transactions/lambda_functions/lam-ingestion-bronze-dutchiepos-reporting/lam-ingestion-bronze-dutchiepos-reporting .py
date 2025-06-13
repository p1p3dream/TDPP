import json
import boto3
import pandas as pd
import awswrangler as wr
import snowflake.connector
import time

s3_client = boto3.client('s3')

# Snowflake db info
secret_name = 'sfsecret'
region_name = 'us-east-1'
sfdb = 'TDP_DEV'
sfschema = 'BRONZE'
sfstage = 'AWS_S3_STAGE_BRONZE'
sfrole = 'SERVICE_ROLE'

# Primary keys to be propagated to child tables in api response
prim_keys = {
    "transactions" : ["transactionId"],
    "transactions_items" : ["transactionItemId"],
    "products": ["productId"]
}

# Fields where value is an array of values. When value is null instead of array need to exclude from being added to parent
# Would like to find a better solution to this
arr_fields = {
    'transactions': {'orderIds', 'discounts', 'feesAndDonations', 'taxSummary', 'manualPayments', 'integratedPayments'},
    'transactions_items': {'discounts', 'taxes'},
    'products': {'imageUrls', 'taxCategories', 'tags', 'pricingTierData', 'broadcastedResponses'}
}

# Create a Secrets Manager client
session = boto3.session.Session()
secret_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


def denormalize(json_data, main_table, request_id):
    items = unnest(json_data, main_table, request_id=request_id)
    print_log(request_id, f'[INFO] Number of transactions processed: {len(json_data)}')
    return items

def get_metadata(json_data, request_id):
    try:
        metadata = {}
        response = {}
        for key in json_data:
            if key == 'response':
                response = json_data[key]
            elif isinstance(json_data[key], dict):
                for param in json_data[key]:
                    metadata[key + '_' + param] = json_data[key][param]
            else:
                metadata[key] = json_data[key]

        if not response:
            print_log(request_id, '[ERROR] No response found when retrieving metadata')
            raise Exception('[ERROR] No response found when retrieving metadata')
        
        return metadata, response
    except Exception as e:
        print_log(request_id, f'[ERROR] Error retrieving metadata: {e}')
        raise Exception(e)
        
def unnest(obj, parent, key_field: str="", primary: dict=None, request_id: str=""):
    items = {}

    if isinstance(obj, dict):      
        # Get current primary keys needed
        if parent in prim_keys:
            keys = prim_keys[parent]
            primary_vals = {}
            if primary:
                for key, val in primary.items():
                    primary_vals[key] = val
                
            for key in keys:
                if key not in primary_vals:
                    primary_vals[key] = obj[key]    
        else:
            primary_vals=None  

        for field, val in obj.items():
            child_items = unnest(val, parent, key_field=field, primary=primary_vals, request_id=request_id)

            # Add item returned to items dict
            for table in child_items:
                if table not in items:
                    items[table] = {}

                for field in child_items[table]:
                    if field in items[table]:
                        items[table][field].extend(child_items[table][field])
                    else:
                        items[table][field] = child_items[table][field]
        # Return to parent level
        return items

    elif isinstance(obj, list):
        if key_field:
            new_parent = parent + "_" + key_field
        else:
            new_parent = parent
            
        for item in obj:
            if isinstance(item, dict):
                if primary is not None:
                    try:
                        # Add primary keys to child for however many array items there are, only if child doesn't already contain them
                        for prim, val in primary.items():
                            if prim not in item.keys():
                                # Child needs the primary key, so add to items
                                if new_parent in items:
                                    if prim in items[new_parent]:
                                        items[new_parent][prim].append(val)
                                    else:
                                        items[new_parent][prim] = [val]
                                else:
                                    items[new_parent] = {
                                        prim : [val]
                                    }
                    except Exception as e:
                        print_log(request_id, f'[ERROR] item object: {item}')
                        print_log(request_id, f'Parent: {parent}')
                        print_log(request_id, f'New Parent: {new_parent}')
                        print_log(request_id, f'Primary keys: {primary}')
                        raise Exception
            else:
                if primary is not None:
                    try:
                        # Add primary keys to child for however many array items there are
                        for prim, val in primary.items():
                            if new_parent in items:
                                if prim in items[new_parent]:
                                    items[new_parent][prim].append(val)
                                else:
                                    items[new_parent][prim] = [val]
                            else:
                                items[new_parent] = {
                                    prim : [val]
                                }
                    except Exception as e:
                        print_log(request_id, f'[ERROR] item object: {item}')
                        print_log(request_id, f'Parent: {parent}')
                        print_log(request_id, f'New Parent: {new_parent}')
                        print_log(request_id, f'Primary keys: {primary}')
                        raise Exception
                
            # Unnest next level
            child_items = unnest(item, new_parent, key_field=key_field, primary=primary, request_id=request_id)

            # Add item returned to items dict
            for table in child_items:
                if table not in items:
                    items[table] = {}

                for field in child_items[table]:
                    if field in items[table]:
                        items[table][field].extend(child_items[table][field])
                    else:
                        items[table][field] = child_items[table][field]
        # Return to parent level
        return items                
    else:
        if parent in arr_fields:
            # If value is null, only add to record if field is not supposed to return an array
            if key_field in arr_fields[parent] and obj is None:
                return {}
            
        item = {
                parent : {
                    key_field : [obj]
                }
            }
        return item

        
def get_snowflake_cred(secret_name, request_id):
    try:
        # Get secret values
        get_secret_value_response = secret_client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except Exception as e:
        print_log(request_id, f'[ERROR] Failed to get snowflake secret. Secret: {secret_name}')
        raise Exception(f'{request_id} {str(e)}')
    
def create_snowflake_conn(secret, request_id):
    try:
        # Create connection to Snowflake
        conn = snowflake.connector.connect(
                user=secret['USERNAME'],
                password=secret['PASSWORD'],
                account=secret['sfaccount'],
                warehouse=secret['sfWarehouse'],
                database=sfdb,
                schema=sfschema,
                role=sfrole,
                session_parameters = {
                    'QUERY_TAG' : 'aws-lam-ingestion-bronze-dutchiepos-reporting'
                }
            )
        return conn
    except Exception as e:
        print_log(request_id, f'[ERROR] Failed to connect to snowflake. Secret: {secret_name}')
        raise Exception(f'{request_id} {str(e)}')
        
def copy_to_snowflake(sfdb, sfschema, sfstage, sftable, file_name, conn, request_id):
    cursor = conn.cursor()
        
    # Create table if not exists, inferring schema from parquet file
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {sfdb}.{sfschema}.{sftable}
          USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
              FROM TABLE(
                INFER_SCHEMA(
                  LOCATION=>'@{sfdb}.{sfschema}.{sfstage}/{file_name}',
                  FILE_FORMAT=>'tdp_parquet_format'
                )
              ));
    """
    
    cursor.execute(create_table_query)
    
    copy_query = f"""
        COPY INTO {sfdb}.{sfschema}.{sftable}
            FROM '@{sfdb}.{sfschema}.{sfstage}/{file_name}'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = case_insensitive
            FORCE = TRUE;
    """
    # print(copy_query)
    
    cursor.execute(copy_query)
    conn.commit()
    print_log(request_id, f'[INFO] Inserted {file_name} into snowflake. Database: {sfdb}, Schema: {sfschema}, Table: {sftable}')

def print_log(request_id, message):
    print(f"[{request_id}] {message}")

def lambda_handler(event, context):
    request_id = context.aws_request_id

    # Get snowflake secret
    secret = get_snowflake_cred(secret_name, request_id)
    conn = create_snowflake_conn(secret, request_id)

    for record in event['Records']:
        try:
            # Get file location from event
            s3_event = json.loads(record["body"])
            s3_bucket_name = s3_event["bucket"]
            s3_file_name = s3_event["key"]
        except Exception as e:
            print_log(request_id, f"[ERROR] Failed to get bucket/file information from event:{event}")
            raise Exception(f'{request_id} {str(e)}')

        # Get file info from event
        # s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        # s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        print_log(request_id, f'Getting data {s3_file_name} in bucket {s3_bucket_name}')
        
        # Decode file into dictionary
        object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        body = object['Body']
        json_string = body.read().decode('utf-8')
        json_data = json.loads(json_string)
        
        # Get table name and source system from file directory
        table_name = s3_file_name.split('/')[2]
        source_system = s3_file_name.split('/')[1]
        file_name = s3_file_name.split('/')[-1][:-5]
        print_log(request_id, f'[INFO] Table Name: {table_name}')
        print_log(request_id, f'[INFO] Source System: {source_system}')
        
        # Split json payload into metadata and response
        metadata, response = get_metadata(json_data, request_id)
        client_id = metadata.get('clientid', 'unknown')

        # Recursively denormalize json    
        json_denorm = denormalize(response, table_name, request_id)
        
        # Write each table data to s3 as parquet
        for table in json_denorm:
            
            # Convert denormalized dictionary into dataframe
            try:
                frame = pd.DataFrame.from_dict(json_denorm[table])
            except Exception as e:
                print_log(request_id, '[ERROR] could not convert to dataframe')
                print_log(request_id, json_denorm[table])
                print_log(request_id, '--------------------------------')
                for field in json_denorm[table]:
                    print_log(request_id, f'Record count for {field}: {len(json_denorm[table][field])}')
                raise Exception(f'{request_id} {str(e)}')

            # Add file path as record metadata, would be helpful if snowflake write fails to track down which files still need to be inserted
            s3_path = f's3://tdp-bronze/{source_system}/{table}/{client_id}/{file_name}.parquet'
            frame['s3_file'] = s3_path
            
            # Add all metadata to dataframe for parent table, or just key metadata for child tables
            if table == table_name:
                for col in metadata:
                    frame[col] = metadata[col]
            else:
                frame['clientid'] = client_id 
                frame['last_processed_datetime_utc'] = metadata['last_processed_datetime_utc']

            # Write dataframe to s3 as parquet
            wr.s3.to_parquet(df=frame, path=s3_path, index=False, boto3_session=session, compression='snappy')
            
            # Copy file to snowflake
            sftable = source_system + '_' + table
            bronze_file_name = f'{source_system}/{table}/{client_id}/{file_name}.parquet'
            try:
                copy_to_snowflake(sfdb, sfschema, sfstage, sftable.upper(), bronze_file_name, conn, request_id)
            except Exception as e: 
                print_log(request_id, f'[ERROR] Error copying file {s3_path} to snowflake: {e}')
                raise Exception(f'{request_id} {str(e)}')
        
    conn.close()
