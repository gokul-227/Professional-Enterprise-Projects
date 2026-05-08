from __future__ import annotations
import uuid
import re
import boto3
import json
import pprint
from datetime import datetime
from airflow import DAG
import json
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from botocore.waiter import WaiterModel, create_waiter_with_client
from botocore.exceptions import WaiterError

# --- Constants ---
RETENTION_UPDATE_GLUE_JOB_NAME = "natwest-archive-data-retention-policy-update"
ASSUME_ROLE_ARN = "arn:aws:iam::934336705194:role/DOC-Airflow-role-dev"
DYNAMODB_TABLE_NAME = "natwest-retention-update-configs"

# --------------------------------------------------------------------
# --- Shared Helper Functions (kept from archive_dag.py)
# --------------------------------------------------------------------

def get_glue_job_run_waiter(glue_client):
    """Creates and returns a custom Boto3 waiter for Glue job run completion."""
    waiter_name = "JobRunCompleted"
    waiter_config = {
        "version": 2,
        "waiters": {
            "JobRunCompleted": {
                "operation": "GetJobRun",
                "delay": 30,
                "maxAttempts": 100,
                "acceptors": [
                    {"matcher": "path", "expected": "SUCCEEDED", "argument": "JobRun.JobRunState", "state": "success"},
                    {"matcher": "path", "expected": "FAILED", "argument": "JobRun.JobRunState", "state": "failure"},
                    {"matcher": "path", "expected": "STOPPED", "argument": "JobRun.JobRunState", "state": "failure"},
                    {"matcher": "path", "expected": "TIMEOUT", "argument": "JobRun.JobRunState", "state": "failure"},
                ],
            }
        },
    }
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, glue_client)


def get_boto3_session():
    """Assume cross-account IAM role and return boto3 session"""
    sts = boto3.client("sts", region_name="eu-west-1", endpoint_url="https://sts.eu-west-1.amazonaws.com")
    creds = sts.assume_role(RoleArn=ASSUME_ROLE_ARN, RoleSessionName="airflow-cross-account")["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="eu-west-1",
    )

# --------------------------------------------------------------------
# --- DAG-Specific Functions
# --------------------------------------------------------------------

def print_config(**context):
    conf = context["dag_run"].conf or {}
    print("=== RAW CONF dict ===")
    pprint.pprint(conf)
    if conf:
        print("=== JSON SERIALIZED ===")
        print(json.dumps(conf, indent=2))
    else:
        print("No configuration received in dag_run.conf")


def parse_and_validate_config(**context):
    conf = context["dag_run"].conf
    if not conf:
        raise AirflowException("Configuration is empty. Cannot proceed.")

    required_keys = ["env", "retention_policy_value", "legal_hold", "target_database", "target_tables"]
    for key in required_keys:
        if key not in conf:
            raise AirflowException(f"Missing key: '{key}'")

    if not isinstance(conf["legal_hold"], bool):
        raise AirflowException("'legal_hold' must be a boolean")

    # Optional fields: email and triggered_by
    conf.setdefault("email", "not_provided")
    conf.setdefault("triggered_by", "system")

    print("Configuration validated successfully.")
    context["ti"].xcom_push(key="validated_config", value=conf)

def ensure_dynamodb_table_exists():
    """Ensure DynamoDB table exists (same logic reused)."""
    session = get_boto3_session()
    dynamodb = session.resource("dynamodb")
    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        table.load()
        print(f"Table {DYNAMODB_TABLE_NAME} already exists")
        return table
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"Creating table {DYNAMODB_TABLE_NAME}...")
        table = dynamodb.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[{"AttributeName": "source_name", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_name", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        print(f"Table {DYNAMODB_TABLE_NAME} created successfully")
        return table


def discover_tables_to_update_retention_policy(**context):
    """
    Discover Glue tables in the given target_database that match
    the include/exclude patterns and return table names for processing.
    """
    session = get_boto3_session()
    glue_client = session.client("glue")

    ti = context["ti"]
    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")

    target_db = config["target_database"]
    target_tables_config = config.get("target_tables", {})
    include = target_tables_config.get("include", [])
    exclude = target_tables_config.get("exclude", [])

    
    
    
    #include = config.get("include", [])
    #exclude = config.get("exclude", [])
    print(f"Scanning Glue DB: {target_db}")

    tables = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=target_db):
        for tbl in page["TableList"]:
            name = tbl["Name"]
            tables.append(name)

    print(f"Found {len(tables)} tables in Glue DB '{target_db}'")

    def match_pattern(name, patterns):
        if not patterns:
            return False
        if "*" in patterns or ".*" in patterns:
            return True
        for p in patterns:
            if re.fullmatch(p.replace("*", ".*"), name):
                return True
        return False

    # Filter by include/exclude
    included = [t for t in tables if match_pattern(t, include) or not include]
    final_list = [t for t in included if not match_pattern(t, exclude)]

    print(f"Final tables for retention policy update: {final_list}")
    ti.xcom_push(key="target_tables", value=final_list)


def prepare_glue_job_args(**context):
    """Prepare Glue job arguments for each target table."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    tables = ti.xcom_pull(task_ids="discover_tables_to_update_retention_policy", key="target_tables")

    if not tables:
        print("No tables found for retention update.")
        return []

    args = []
    for t in tables:
        glue_args = {
            "--target_glue_db": config["target_database"],
            "--target_glue_table": t,
            "--retention_policy_value": config["retention_policy_value"],
            "--legal_hold": str(config["legal_hold"]).lower(),
        }
        args.append(glue_args)

    print(f"Prepared {len(args)} Glue argument sets.")
    return args


@task(task_id="run_retention_policy_update_job", max_active_tis_per_dag=3)
def run_glue_job(script_args):
    """Trigger and monitor retention policy Glue job"""
    session = get_boto3_session()
    glue = session.client("glue")
    print("Running Glue with args:")
    pprint.pprint(script_args)

    response = glue.start_job_run(JobName=RETENTION_UPDATE_GLUE_JOB_NAME, Arguments=script_args)
    job_run_id = response["JobRunId"]
    print(f"Started Glue job run: {job_run_id}")

    waiter = get_glue_job_run_waiter(glue)
    try:
        waiter.wait(JobName=RETENTION_UPDATE_GLUE_JOB_NAME, RunId=job_run_id)
    except WaiterError as e:
        print(f"Glue Waiter failed: {e}")

    result = glue.get_job_run(JobName=RETENTION_UPDATE_GLUE_JOB_NAME, RunId=job_run_id)
    job = result["JobRun"]
    status = job["JobRunState"]
    start = job["StartedOn"]
    end = job["CompletedOn"]
    duration = int((end - start).total_seconds())

    metadata = {
        "job_name": RETENTION_UPDATE_GLUE_JOB_NAME,
        "job_run_id": job_run_id,
        "status": status,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "duration_seconds": duration,
        "script_args": script_args,
    }

    print("Glue Job Metadata:", metadata)
    if status != "SUCCEEDED":
        raise AirflowException(f"Glue job failed with status: {status}")
    return metadata


def aggregate_job_metadata(**context):
    """Collect and aggregate metadata from all retention Glue job runs."""
    ti = context["ti"]

    # Pull all XComs returned by the dynamic tasks
    results = ti.xcom_pull(task_ids="run_retention_policy_update_job")

    # Handle any Airflow LazyXComSequence, None, or single-dict return types
    if not results:
        print(" No job results found — possibly all failed or skipped.")
        results = []
    elif isinstance(results, dict):
        results = [results]
    elif not isinstance(results, (list, tuple)):
        try:
            results = list(results)
        except Exception:
            results = [results]

    # Filter out any None entries
    results = [r for r in results if r]

    # Aggregate metadata
    metadata = {
        "timestamp": datetime.utcnow().isoformat(),
        "job_count": len(results),
        "retention_jobs": results,
    }

    print("Aggregated Glue job metadata:")
    print(json.dumps(metadata, indent=2, default=str))

    # Push to XCom for the next task
    ti.xcom_push(key="job_metadata", value=metadata)
    return metadata
    
    
def store_config_in_dynamodb(**context):
    """Store aggregated retention update job metadata in DynamoDB."""
    print("--- Storing retention update metadata ---")

    table = ensure_dynamodb_table_exists()
    ti = context["ti"]

    run_id = str(uuid.uuid4())
    created_date = datetime.utcnow().isoformat()

    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    job_metadata = ti.xcom_pull(task_ids="aggregate_job_metadata", key="job_metadata") or {}

    retention_jobs = job_metadata.get("retention_jobs", [])
    dag_status = "succeeded" if all(j.get("status") == "SUCCEEDED" for j in retention_jobs) else "failed"

    # Build flattened summary of all tables
    all_tables = []
    for job in retention_jobs:
        args = job.get("script_args", {})
        tbl = args.get("--target_glue_table")
        if tbl:
            all_tables.append(tbl)

    item = {
        "id": run_id,
        "source_name": config["target_database"],
        "env": config["env"],
        "created_date": created_date,
        "dag_status": dag_status,
        "input_retention_policy": config["retention_policy_value"],
        "input_legal_hold": str(config["legal_hold"]).lower(),
        "target_tables": ",".join(all_tables),
        "job_metadata": json.dumps(job_metadata, default=str),
        # Optional fields
        "email": config.get("email", "not_provided"),
        "triggered_by": config.get("triggered_by", "system"),
    }

    try:
        table.put_item(Item=item)
        print(f"Stored metadata for {len(retention_jobs)} Glue job(s) in DynamoDB.")
    except Exception as e:
        raise AirflowException(f"Failed to store metadata in DynamoDB: {str(e)}")

    return item

# --------------------------------------------------------------------
# --- DAG Definition
# --------------------------------------------------------------------

default_args = {"owner": "natwest", "start_date": datetime(2025, 1, 1)}

with DAG(
    dag_id="natwest_retention_update_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retention", "update", "triggered"],
) as dag:

    print_raw_config = PythonOperator(task_id="print_config", python_callable=print_config)
    validate_config = PythonOperator(task_id="parse_and_validate_config", python_callable=parse_and_validate_config)
    discover_tables = PythonOperator(
        task_id="discover_tables_to_update_retention_policy",
        python_callable=discover_tables_to_update_retention_policy,
    )
    prepare_args = PythonOperator(task_id="prepare_glue_job_args", python_callable=prepare_glue_job_args)
    aggregate_job_metadata_task = PythonOperator(task_id="aggregate_job_metadata", python_callable=aggregate_job_metadata)
    store_config = PythonOperator(task_id="store_config_in_dynamodb", python_callable=store_config_in_dynamodb)

    run_retention_jobs = run_glue_job.expand(script_args=prepare_args.output)

    (
        print_raw_config
        >> validate_config
        >> discover_tables
        >> prepare_args
        >> run_retention_jobs
        >> aggregate_job_metadata_task
        >> store_config
    )
