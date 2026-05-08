from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from botocore.waiter import WaiterModel, create_waiter_with_client
from botocore.exceptions import WaiterError
from datetime import datetime
import json
import pprint
import boto3

# --- Constants ---
PURGE_GLUE_JOB_NAME = "natwest-data-purge-from-iceberg"
ASSUME_ROLE_ARN = "arn:aws:iam::934336705194:role/DOC-Airflow-role-dev"

# --- Create a dedicated helper function for the waiter ---
def get_glue_job_run_waiter(glue_client):
    """
    Creates and returns a custom Boto3 waiter for Glue job run completion.
    """
    waiter_name = "JobRunCompleted"
    waiter_config = {
        "version": 2,
        "waiters": {
            "JobRunCompleted": {
                "operation": "GetJobRun",
                "delay": 30,
                "maxAttempts": 100,
                "acceptors": [
                    {
                        "matcher": "path",
                        "expected": "SUCCEEDED",
                        "argument": "JobRun.JobRunState",
                        "state": "success"
                    },
                    {
                        "matcher": "path",
                        "expected": "FAILED",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "STOPPED",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "TIMEOUT",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    }
                ]
            }
        }
    }
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, glue_client)

# --- Cross-account boto3 session ---
def get_boto3_session():
    sts = boto3.client(
        "sts",
        region_name="eu-west-1",
        endpoint_url="https://sts.eu-west-1.amazonaws.com"
        )
    creds = sts.assume_role(
        RoleArn=ASSUME_ROLE_ARN,
        RoleSessionName="airflow-cross-account"
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="eu-west-1"
    )

# --- Task Functions ---

def print_purge_config(**context):
    """Print the purge configuration"""
    conf = context["dag_run"].conf or {}
    print("=== PURGE CONFIG ===")
    pprint.pprint(conf)
    if conf:
        print("=== JSON SERIALIZED ===")
        print(json.dumps(conf, indent=2))
    else:
        print("No configuration received in dag_run.conf")

def parse_and_validate_purge_config(**context):
    """Validate purge configuration and extract target info"""
    print("--- Starting purge configuration validation ---")
    conf = context["dag_run"].conf
    if not conf:
        raise AirflowException("Configuration is empty. Cannot proceed.")

    if 'targets' not in conf:
        raise AirflowException("Configuration missing 'targets' section")
    
    targets = conf['targets']
    if not targets or len(targets) == 0:
        raise AirflowException("No targets specified in configuration")
    
    # For now, support single target
    target = targets[0]
    
    required_keys = ["source_name", "connection"]
    for key in required_keys:
        if key not in target:
            raise AirflowException(f"Target missing required key: '{key}'")
    
    source_name = target['source_name']
    connection = target['connection']
    iceberg_database = f"archive_{connection}"
    
    print(f"Validated purge target:")
    print(f"  Source Name: {source_name}")
    print(f"  Connection: {connection}")
    print(f"  Iceberg Database: {iceberg_database}")
    
    # Store in XCom for downstream tasks
    context["ti"].xcom_push(key="source_name", value=source_name)
    context["ti"].xcom_push(key="connection", value=connection)
    context["ti"].xcom_push(key="iceberg_database", value=iceberg_database)
    context["ti"].xcom_push(key="target_config", value=target)

def run_discovery_glue_job(**context):
    """Run Glue job in discovery mode to find tables with expired data"""
    print("--- Starting Discovery Glue Job ---")
    
    ti = context["ti"]
    source_name = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="source_name")
    connection = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="connection")
    iceberg_database = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="iceberg_database")
    
    session = get_boto3_session()
    glue = session.client("glue")
    
    # Prepare arguments for discovery mode
    script_args = {
        "--mode": "discovery",
        "--source_name": source_name,
        "--connection": connection,
        "--iceberg_database": iceberg_database
    }
    
    print("Running Discovery Glue job with args:")
    pprint.pprint(script_args)
    
    response = glue.start_job_run(
        JobName=PURGE_GLUE_JOB_NAME,
        Arguments=script_args
    )
    
    job_run_id = response["JobRunId"]
    print(f"Started Discovery Glue job. Run ID: {job_run_id}")

    # Wait for completion using custom waiter
    custom_waiter = get_glue_job_run_waiter(glue)
    
    print("Waiting for Discovery Glue job to complete...")
    try:
        custom_waiter.wait(JobName=PURGE_GLUE_JOB_NAME, RunId=job_run_id)
        print("Discovery Glue job succeeded.")
        
        # Store job run ID for later reference
        context["ti"].xcom_push(key="discovery_job_run_id", value=job_run_id)
        
    except WaiterError as e:
        print(f"Waiter failed: {e}")
        status_response = glue.get_job_run(JobName=PURGE_GLUE_JOB_NAME, RunId=job_run_id)
        job_status = status_response["JobRun"]["JobRunState"]
        error_message = status_response["JobRun"].get("ErrorMessage", "No error message provided.")
        raise AirflowException(f"Discovery Glue job failed with status '{job_status}'. Error: {error_message}")

def wait_for_manual_approval(**context):
    """
    Manual approval step - user must mark this task as success
    """
    ti = context["ti"]
    discovery_job_run_id = ti.xcom_pull(task_ids="discover_tables_to_purge", key="discovery_job_run_id")
    
    print("=== MANUAL APPROVAL REQUIRED ===")
    print(f"Discovery Job Run ID: {discovery_job_run_id}")
    print("")
    print("INSTRUCTIONS:")
    print("1. Go to AWS Glue Console")
    print("2. Review the discovery job logs")
    print("3. Check what data will be purged")
    print("4. If you approve: Mark this task as SUCCESS")
    print("5. If you reject: Mark this task as FAILED")
    print("")
    print("⚠️  This task will remain RUNNING until you manually mark it")
    print("⚠️  Only approve if you want to proceed with data deletion!")
    
    # This will keep the task running indefinitely until manually marked
    import time
    while True:
        time.sleep(300)  # Sleep for 5 minutes, then check again
        print("Still waiting for manual approval...")

# Replace the DummyOperator with:
approval_checkpoint = PythonOperator(
    task_id="approval_checkpoint",
    python_callable=wait_for_manual_approval,
)

def run_purge_glue_job(**context):
    """Run Glue job in delete mode to purge expired data"""
    print("--- Starting Purge Glue Job ---")
    
    ti = context["ti"]
    source_name = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="source_name")
    connection = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="connection")
    iceberg_database = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="iceberg_database")
    target_config = ti.xcom_pull(task_ids="parse_and_validate_purge_config", key="target_config")
    
    session = get_boto3_session()
    glue = session.client("glue")
    
    # Prepare arguments for delete mode
    script_args = {
        "--mode": "delete",
        "--source_name": source_name,
        "--connection": connection,
        "--iceberg_database": iceberg_database,
        "--max_parallel_tables": str(target_config.get("max_parallel_tables", 5))
    }
    
    print("Running Purge Glue job with args:")
    pprint.pprint(script_args)
    
    response = glue.start_job_run(
        JobName=PURGE_GLUE_JOB_NAME,
        Arguments=script_args
    )
    
    job_run_id = response["JobRunId"]
    print(f"Started Purge Glue job. Run ID: {job_run_id}")

    # Wait for completion using custom waiter
    custom_waiter = get_glue_job_run_waiter(glue)
    
    print("Waiting for Purge Glue job to complete...")
    try:
        custom_waiter.wait(JobName=PURGE_GLUE_JOB_NAME, RunId=job_run_id)
        print("Purge Glue job succeeded.")
        
    except WaiterError as e:
        print(f"Waiter failed: {e}")
        status_response = glue.get_job_run(JobName=PURGE_GLUE_JOB_NAME, RunId=job_run_id)
        job_status = status_response["JobRun"]["JobRunState"]
        error_message = status_response["JobRun"].get("ErrorMessage", "No error message provided.")
        raise AirflowException(f"Purge Glue job failed with status '{job_status}'. Error: {error_message}")

# --- DAG Definition ---

default_args = {
    "owner": "natwest",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="natwest_data_purge_from_iceberg",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["purge", "iceberg", "triggered"],
) as dag:

    print_config = PythonOperator(
        task_id="print_purge_config",
        python_callable=print_purge_config,
    )

    validate_config = PythonOperator(
        task_id="parse_and_validate_purge_config",
        python_callable=parse_and_validate_purge_config,
    )

    discover_tables_to_purge = PythonOperator(
        task_id="discover_tables_to_purge",
        python_callable=run_discovery_glue_job,
    )

    # Manual approval checkpoint
    approval_checkpoint = PythonOperator(
        task_id="approval_checkpoint",
        python_callable=wait_for_manual_approval,
    )

    execute_purge = PythonOperator(
        task_id="execute_purge_operations",
        python_callable=run_purge_glue_job,
    )

    # Simple linear flow - always go to approval
    print_config >> validate_config >> discover_tables_to_purge  >> approval_checkpoint >> execute_purge