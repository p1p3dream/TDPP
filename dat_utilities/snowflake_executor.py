import os
import sys
import argparse
import logging
import traceback
from dat_utilities.utils import (
    find_sql_files,
    execute_sql_file,
    snowflake_connect
)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                  format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Execute Snowflake DDL files')
    parser.add_argument('--continue-on-error', action='store_true',
                      help='Continue executing SQL files even if one fails')
    parser.add_argument('--script', type=str, help='Path to a specific SQL file to execute')
    # Add an environment argument
    parser.add_argument('--environment', type=str, default='dev',
                      help='Deployment environment (dev or prod)')
    return parser.parse_args()

def main():
    try:
        args = parse_args()
        
        # Get environment from command line argument first, falling back to env var
        env = args.environment.lower() if args.environment else os.environ.get('ENVIRONMENT', 'dev').lower()
        
        # Set database based on environment
        database = "TDP_PROD" if env == 'prod' else "TDP_DEV"
        job_prefix = os.environ.get('JOB_PREFIX', '')
        
        logger.info(f"Environment from args: {args.environment}")
        logger.info(f"Environment from env var: {os.environ.get('ENVIRONMENT', 'NOT_SET')}")
        logger.info(f"Final environment used: {env}")
        logger.info(f"Using database: {database}")
        logger.info(f"Job prefix: {job_prefix}")
        
        # Find SQL files
        if args.script:
            sql_files = [args.script]
        else:
            sql_files = find_sql_files(job_prefix)
        
        if not sql_files:
            logger.info("No SQL files to execute")
            return
        
        # Connect to Snowflake
        conn = snowflake_connect(database=database)
        
        try:
            # Execute each SQL file
            for sql_file in sql_files:
                try:
                    logger.info(f"Processing SQL file: {sql_file}")
                    execute_sql_file(conn, sql_file, database)
                    logger.info(f"Successfully executed SQL file: {sql_file}")
                except Exception as e:
                    logger.error(f"Error executing SQL file {sql_file}: {e}")
                    logger.error(traceback.format_exc())
                    # Continue with next file instead of exiting if flag is set
                    if args.continue_on_error:
                        logger.warning("Continuing to next SQL file due to --continue-on-error flag")
                        continue
                    else:
                        raise
            
            logger.info("All SQL files executed successfully")
        finally:
            conn.close()
            logger.info("Snowflake connection closed")
            
        logger.info("DDL execution completed successfully")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)