import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Base directory of the Snowflake DDL folder
BASE_DIR = "/path/to/snowflake_ddl"  # Replace with the path to your snowflake_ddl folder
SNOWFLAKE_CONN_ID = "snowflake_default"

# Helper function to recursively find all SQL files with their associated database name
def load_sql_files_with_db(base_dir):
    sql_files_with_db = {}
    for root, _, files in os.walk(base_dir):
        for file_name in files:
            if file_name.endswith(".sql"):
                full_path = os.path.join(root, file_name)
                # Extract database name from the folder structure (e.g., `databases/<DB_NAME>`)
                relative_path = os.path.relpath(full_path, base_dir)
                path_parts = relative_path.split(os.sep)
                if "databases" in path_parts:
                    db_index = path_parts.index("databases") + 1
                    if db_index < len(path_parts):
                        db_name = path_parts[db_index]
                        sql_files_with_db[full_path] = db_name
    return sql_files_with_db

# Function to generate task IDs based on the file path
def generate_task_id(file_path, base_dir):
    # Generate task ID based on relative path within BASE_DIR
    relative_path = os.path.relpath(file_path, base_dir)
    return relative_path.replace(os.sep, "_").replace(".", "_")

# Load SQL files and their associated database names
sql_files_with_db = load_sql_files_with_db(BASE_DIR)

# Initialize the DAG
with DAG(
    dag_id='refactored_snowflake_ddl_with_db',
    default_args=default_args,
    description='Execute Snowflake DDL tasks dynamically based on new folder structure with DB name',
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Create a task for each SQL file
    sql_tasks = []
    for sql_file, db_name in sql_files_with_db.items():
        task_id = generate_task_id(sql_file, BASE_DIR)
        with open(sql_file, "r") as file:
            sql_content = file.read()
        
        task = SnowflakeOperator(
            task_id=task_id,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=sql_content,
            params={"db_name": db_name},  # Pass the database name as a parameter
        )
        sql_tasks.append(task)

    # Optionally, define dependencies between tasks if needed
    for i in range(len(sql_tasks) - 1):
        sql_tasks[i] >> sql_tasks[i + 1]
