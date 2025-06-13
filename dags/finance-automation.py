from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import boto3
import json

# Configuration class for finance jobs
class FinanceConfig:
    def __init__(self, glue_id, state, report_name, flag_active=True):
        self.glue_id = glue_id
        self.state = state
        self.report_name = report_name
        self.flag_active = flag_active

    def to_dict(self):
        return {
            'glue_id': self.glue_id,
            'state': self.state,
            'report_name': self.report_name,
            'flag_active': self.flag_active
        }

# Load configurations - in production, this could be stored in Airflow Variables
FINANCE_CONFIGS = [
    FinanceConfig("finance_daily", "NY,CA,TX", "daily_revenue"),
    FinanceConfig("finance_monthly", "ALL", "monthly_summary"),
    # Add more configurations as needed
]

def send_sns_notification(message, subject="Airflow Finance Pipeline Alert"):
    """Send SNS notification for important events/errors"""
    sns = boto3.client('sns', region_name='us-east-1')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:076579646618:sns-dat-aws-notifications',
        Message=message,
        Subject=subject
    )

def on_failure_callback(context):
    """Callback function for task failure"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    error_message = context.get('exception', 'Unknown error')
    
    message = f"""
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {context['execution_date']}
    Error: {error_message}
    """
    send_sns_notification(message, "Airflow Finance Pipeline Failure")

# Define the default_args for the DAG
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

def create_processing_group(config: FinanceConfig):
    """Create a task group for processing a single configuration"""
    with TaskGroup(group_id=f'process_{config.glue_id}') as group:
        
        # Check if the configuration is active
        def check_active(**context):
            if not config.flag_active:
                send_sns_notification(f"Skipping inactive configuration: {config.glue_id}")
                return f'process_{config.glue_id}.skip_processing'
            return f'process_{config.glue_id}.run_glue_job'

        check_status = BranchPythonOperator(
            task_id='check_active',
            python_callable=check_active
        )

        # Skip processing task
        skip_processing = PythonOperator(
            task_id='skip_processing',
            python_callable=lambda: None
        )

        # Glue job execution
        run_glue = GlueJobOperator(
            task_id='run_glue_job',
            job_name='tdp-glue-hana-to-s3-finance',
            script_location='s3://SourceBucketName/TDP/jobs/finance-automation/glue/tdp-glue-hana-to-s3-finance.py',
            script_args={
                '--state': config.state,
                '--report_name': config.report_name,
                '--start_date': "{{ task_instance.xcom_pull(task_ids='invoke_lambda_calculate_dates', key='start_date') }}",
                '--end_date': "{{ task_instance.xcom_pull(task_ids='invoke_lambda_calculate_dates', key='end_date') }}"
            },
            aws_conn_id='aws_default',
            region_name='us-east-1',
            retry_limit=3,
            retry_delay=timedelta(minutes=5)
        )

        # Update process status
        def update_status(**context):
            execution_date = context['execution_date']
            config_dict = config.to_dict()
            config_dict.update({
                'last_run': execution_date.isoformat(),
                'status': 'completed'
            })
            # Store in XCom for tracking
            context['task_instance'].xcom_push(
                key=f'status_{config.glue_id}',
                value=json.dumps(config_dict)
            )

        update_process = PythonOperator(
            task_id='update_status',
            python_callable=update_status,
            provide_context=True
        )

        # Define task dependencies within the group
        check_status >> [skip_processing, run_glue]
        run_glue >> update_process

        return group

# Define the DAG
with DAG(
    'tdp_finance_pipeline',
    default_args=default_args,
    description='Enhanced Finance Data Processing Pipeline for SAP HANA to S3',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['finance', 'etl']
) as dag:

    # Task 1: Archive previous files
    invoke_lambda_s3_to_s3 = LambdaInvokeFunctionOperator(
        task_id='invoke_lambda_s3_to_s3',
        function_name='tdp-lambda-s3-to-s3-finance',
        log_type='None',
        aws_conn_id='aws_default',
        region='us-east-1'
    )

    # Task 2: Calculate dates for the process
    invoke_lambda_calculate_dates = LambdaInvokeFunctionOperator(
        task_id='invoke_lambda_calculate_dates',
        function_name='tdp-lambda-calculate-date-finance',
        log_type='None',
        aws_conn_id='aws_default',
        region='us-east-1'

    )

    # Create processing groups for each configuration
    processing_groups = [create_processing_group(config) for config in FINANCE_CONFIGS]

    # Final status check
    def check_final_status(**context):
        """Check the status of all processing groups and send notification"""
        all_statuses = {}
        for config in FINANCE_CONFIGS:
            status = context['task_instance'].xcom_pull(
                key=f'status_{config.glue_id}',
                task_ids=f'process_{config.glue_id}.update_status'
            )
            if status:
                all_statuses[config.glue_id] = json.loads(status)
        
        send_sns_notification(
            f"Finance Pipeline Completed\nProcessed configurations: {json.dumps(all_statuses, indent=2)}",
            "Finance Pipeline Status"
        )

    final_status = PythonOperator(
        task_id='final_status',
        python_callable=check_final_status,
        provide_context=True
    )

    # Define the main DAG structure
    invoke_lambda_s3_to_s3 >> invoke_lambda_calculate_dates >> processing_groups >> final_status
