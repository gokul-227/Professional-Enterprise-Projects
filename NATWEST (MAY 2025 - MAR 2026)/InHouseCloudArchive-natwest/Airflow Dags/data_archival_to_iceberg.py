from __future__ import annotations
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from botocore.waiter import WaiterModel, create_waiter_with_client
from botocore.exceptions import WaiterError
from datetime import datetime, timedelta
import json
import pprint
import boto3

# --- Constants ---
DISCOVER_LAMBDA_FUNCTION_NAME = "natwest_data_archive_discover_tables"
ARCHIVE_GLUE_JOB_NAME = "natwest-data-archive-table-to-iceberg"
VALIDATE_GLUE_JOB_NAME = "natwest-archive-data-validation"
REPORT_GENERATION_LAMBDA_NAME = "natwest_data_archive_validation_report_generation"
ASSUME_ROLE_ARN = "arn:aws:iam::934336705194:role/DOC-Airflow-role-dev"
DYNAMODB_TABLE_NAME = "natwest-archival-configs"

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

    required_keys = ["env", "warehouses", "connections", "sources", "retention_policies"]
    for key in required_keys:
        if key not in conf:
            raise AirflowException(f"Missing key: '{key}'")

    for i, source in enumerate(conf["sources"]):
        wh = source.get("warehouse")
        if not wh or wh not in conf["warehouses"]:
            raise AirflowException(f"Source #{i}: unknown warehouse '{wh}'")

        conn = source.get("connection")
        if not conn or conn not in conf["connections"]:
            raise AirflowException(f"Source #{i}: unknown connection '{conn}'")

        legal_hold = source.get("legal_hold")
        if legal_hold is None or not isinstance(legal_hold, bool):
            raise AirflowException(f"Source #{i}: 'legal_hold' must be a Boolean.")

        consumer_roles = source.get("consumer_roles")
        if consumer_roles is not None:
            if not isinstance(consumer_roles, list) or not consumer_roles:
                raise AirflowException(
                    f"Source #{i}: 'consumer_roles' must be a non-empty list when provided."
                )
            invalid_roles = [
                role for role in consumer_roles if not isinstance(role, str) or not role.strip()
            ]
            if invalid_roles:
                raise AirflowException(
                    f"Source #{i}: 'consumer_roles' entries must be non-empty strings."
                )

        include = source.get("include", {})
        schemas = include.get("schemas", [])
        if not isinstance(schemas, list) or not schemas:
            raise AirflowException(f"Source #{i}: 'include.schemas' must be a non-empty list.")

        for sch in schemas:
            if "name" not in sch:
                raise AirflowException(f"Source #{i}: every schema requires a 'name'.")
            tables = sch.get("tables", [])
            if not isinstance(tables, list):
                raise AirflowException(f"Source #{i} schema '{sch.get('name')}': 'tables' must be a list.")
            for t in tables:
                has_name = "name" in t and isinstance(t["name"], str) and t["name"].strip() != ""
                has_query = "query" in t and isinstance(t["query"], str) and t["query"].strip() != ""
                if has_name == has_query:
                    raise AirflowException(
                        f"Source #{i} schema '{sch['name']}': each table entry must have exactly one of "
                        "'name' or 'query'. Entry: {t}"
                    )
                if has_query:
                    # query-rules must declare where we will write in Iceberg
                    tgt = t.get("target_table")
                    if not tgt or not isinstance(tgt, str):
                        raise AirflowException(
                            f"Source #{i} schema '{sch['name']}': query entry requires 'target_table'."
                        )
                    if "condition" in t:
                        raise AirflowException(
                            f"Source #{i} schema '{sch['name']}': do not combine 'query' and 'condition'. "
                            "Put the predicate inside the SQL."
                        )

    print("Configuration validated.")
    context["ti"].xcom_push(key="validated_config", value=conf)

def ensure_dynamodb_table_exists():
    """Create DynamoDB table if it doesn't exist"""
    session = get_boto3_session()
    dynamodb = session.resource('dynamodb')

    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        table.load()  # This will raise an exception if table doesn't exist
        print(f"Table {DYNAMODB_TABLE_NAME} already exists")
        return table
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"Creating table {DYNAMODB_TABLE_NAME}...")
        table = dynamodb.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'source_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'source_name',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Wait for table to be created
        table.wait_until_exists()
        print(f"Table {DYNAMODB_TABLE_NAME} created successfully")
        return table

def store_config_in_dynamodb(**context):
    """
    Store the validated configuration in DynamoDB for future retrieval by purge DAG.
    """
    print("--- Starting DynamoDB config storage ---")
    # Ensure table exists first
    table = ensure_dynamodb_table_exists()

    # Get validated config and DAG run info
    ti = context["ti"]
    conf = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    dag_run_id = context["dag_run"].run_id

    # Extract source name from config
    source_name = conf['sources'][0]['name']
    env = conf.get('env', 'dev')

    try:
        # Store config with source_name as partition key
        table.put_item(
            Item={
                'source_name': source_name,                    # Partition Key
                'env': env,
                'dag_run_id': dag_run_id,
                'archival_date': datetime.now().isoformat(),
                'config': conf
            }
        )

        print(f"Config successfully stored in DynamoDB:")
        print(f"Source: {source_name}")
        print(f"Environment: {env}")
        print(f"DAG Run ID: {dag_run_id}")
        print(f"Table: {DYNAMODB_TABLE_NAME}")

    except Exception as e:
        print(f"Failed to store config in DynamoDB: {str(e)}")
        raise AirflowException(f"DynamoDB storage failed: {str(e)}")

def invoke_discover_lambda(**context):
    session = get_boto3_session()
    lambda_client = session.client("lambda")
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    source_config = config["sources"][0]

    payload = json.dumps(source_config)

    response = lambda_client.invoke(
        FunctionName=DISCOVER_LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=payload.encode("utf-8"),
    )

    raw = response["Payload"].read().decode("utf-8")
    print("Lambda response:", raw)
    ti.xcom_push(key="return_value", value=raw)

def aggregate_job_metadata(**context):
    ti = context["ti"]

    # Resolve LazyXComSelectSequence by converting to list
    archive_results = ti.xcom_pull(task_ids="run_archive_glue_job", key="return_value")
    validate_results = ti.xcom_pull(task_ids="run_validate_glue_job", key="return_value")

    # Make sure they're resolved (if they're Lazy objects)
    if not isinstance(archive_results, list):
        archive_results = list(archive_results)
    if not isinstance(validate_results, list):
        validate_results = list(validate_results)

    dag_run_id = context["dag_run"].run_id

    metadata = {
        "timestamp": datetime.now().isoformat(),
        "airflow_run_id": dag_run_id,
        "archive_jobs": archive_results,
        "validate_jobs": validate_results
    }

    print("Aggregated job metadata:")
    print("Metadata:", metadata)

    # Push only JSON-serializable data
    ti.xcom_push(key="job_metadata", value=metadata)

def invoke_report_lambda(**context):
    session = get_boto3_session()
    lambda_client = session.client("lambda")

    ti = context["ti"]

    # Get job metadata from XCom
    metadata = ti.xcom_pull(task_ids="aggregate_job_metadata", key="job_metadata")

    if not metadata:
        raise AirflowException("No metadata found from aggregate_job_metadata.")
    
    # Use DAG run ID or generate a UUID
    run_id = str(uuid.uuid4())

    payload = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata
    }

    print(f"Invoking report generation Lambda with payload: {json.dumps(payload, indent=2)}")

    response = lambda_client.invoke(
        FunctionName=REPORT_GENERATION_LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    raw = response["Payload"].read().decode("utf-8")
    print("Lambda response:", raw)


def prepare_glue_job_args(**context):
    ti = context["ti"]
    payload_str = ti.xcom_pull(task_ids="discover_tables_to_archive", key="return_value")
    if not payload_str:
        print("Empty Lambda payload. Nothing to do.")
        return []

    payload = json.loads(payload_str)
    discovered = payload.get("discovered_tables", [])
    if not discovered:
        print("No discovered tables.")
        return []

    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    source = config["sources"][0]

    wh_path = config["warehouses"][source["warehouse"]]
    conn_name = source["connection"]
    conn_val = config["connections"][conn_name]  # actual Glue connection name
    retention_value = config["retention_policies"][source["retention_policy"]]
    legal_hold = source['legal_hold']

    args = []
    for t in discovered:
        # Branch: query vs table
        is_query = "query" in t
        schema = t.get("schema")  # present for both kinds
        if is_query:
            query_sql = t["query"]
            target_table = t["target_table"]  # required by validation
            s3_path = f"{wh_path.rstrip('/')}/{conn_name}/{schema}/{target_table}/"

            glue_args = {
                "--glue_connection_name": conn_val,
                "--source_schema": schema,
                "--target_s3_path": s3_path,
                "--target_glue_db": f"archive_{conn_val}",
                "--target_glue_table": target_table,
                "--retention_policy_value": retention_value,
                "--legal_hold": str(legal_hold).lower(),
                "--query": query_sql
            }
            # NOTE: no --source_table, no --condition in query mode
        else:
            table = t["table"]
            condition = t.get("condition")
            s3_path = f"{wh_path.rstrip('/')}/{conn_name}/{schema}/{table}/"

            glue_args = {
                "--source_schema": schema,
                "--source_table": table,
                "--glue_connection_name": conn_val,
                "--target_s3_path": s3_path,
                "--target_glue_db": f"archive_{conn_val}",
                "--target_glue_table": f"{schema}_{table}",
                "--retention_policy_value": retention_value,
                "--legal_hold": str(legal_hold).lower(),
            }
            if condition:
                glue_args["--condition"] = condition

        args.append(glue_args)

    print(f"Prepared args for {len(args)} items (tables and/or queries).")
    return args

@task(task_id="run_archive_glue_job", max_active_tis_per_dag=2)
def run_glue_job(script_args):
    session = get_boto3_session()
    glue = session.client("glue")
    print("Running Glue with args:")
    pprint.pprint(script_args)
    response = glue.start_job_run(
        JobName=ARCHIVE_GLUE_JOB_NAME,
        Arguments=script_args
    )
    job_run_id = response["JobRunId"]
    print(f"Started Glue job. Run ID: {job_run_id}")

    # --- Get and use the custom waiter from the helper function ---
    custom_waiter = get_glue_job_run_waiter(glue)
    
    print("Waiting for Glue job to complete...")
    try:
        custom_waiter.wait(JobName=ARCHIVE_GLUE_JOB_NAME, RunId=job_run_id)
        print("Glue job succeeded.")
    except WaiterError as e:
        print(f"Waiter failed: {e}")

    # Get final job status and details
    status_response = glue.get_job_run(JobName=ARCHIVE_GLUE_JOB_NAME, RunId=job_run_id)
    job_run = status_response["JobRun"]
    job_status = job_run["JobRunState"]
    error_message = job_run.get("ErrorMessage", "No error message provided.")
    start_time = job_run["StartedOn"]
    end_time = job_run["CompletedOn"]
    duration_timedelta = end_time - start_time
    duration_seconds = int(duration_timedelta.total_seconds())
    minutes, seconds = divmod(duration_seconds, 60)
    duration = f"{minutes}m {seconds}s"

    result = {
        "job_name": ARCHIVE_GLUE_JOB_NAME,
        "job_run_id": job_run_id,
        "status": job_status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "script_args": script_args,
    }

    print("Job metadata:", result)

    if job_status != "SUCCEEDED":
        raise AirflowException(f"Glue job failed. Error: {error_message}")

    return result

@task(task_id="run_validate_glue_job", max_active_tis_per_dag=2)
def run_validate_job(script_args):
    session = get_boto3_session()
    glue = session.client("glue")
    print("Running Validation Glue job with args:")
    pprint.pprint(script_args)
    response = glue.start_job_run(JobName=VALIDATE_GLUE_JOB_NAME, Arguments=script_args)
    job_run_id = response["JobRunId"]
    custom_waiter = get_glue_job_run_waiter(glue)
    print("Waiting for Validation Glue job to complete...")
    try:
        custom_waiter.wait(JobName=VALIDATE_GLUE_JOB_NAME, RunId=job_run_id)
        print("Validation Glue job succeeded.")
    except WaiterError as e:
        print(f"Waiter failed: {e}")

    # Get final job status and details
    status_response = glue.get_job_run(JobName=VALIDATE_GLUE_JOB_NAME, RunId=job_run_id)
    job_run = status_response["JobRun"]
    job_status = job_run["JobRunState"]
    error_message = job_run.get("ErrorMessage", "No error message provided.")
    start_time = job_run["StartedOn"]
    end_time = job_run["CompletedOn"]
    duration_timedelta = end_time - start_time
    duration_seconds = int(duration_timedelta.total_seconds())
    minutes, seconds = divmod(duration_seconds, 60)
    duration = f"{minutes}m {seconds}s"


    result = {
        "job_name": VALIDATE_GLUE_JOB_NAME,
        "job_run_id": job_run_id,
        "status": job_status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration,
        "script_args": script_args,
    }

    print("Validation job metadata:", result)

    if job_status != "SUCCEEDED":
        raise AirflowException(f"Validation Glue job failed. Error: {error_message}")

    return result
# --- DAG Definition ---

default_args = {
    "owner": "natwest",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="natwest_data_archival_to_iceberg",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["archival", "triggered"],
) as dag:

    print_raw_config = PythonOperator(
        task_id="print_config",
        python_callable=print_config,
    )

    validate_config = PythonOperator(
        task_id="parse_and_validate_config",
        python_callable=parse_and_validate_config,
    )

    store_config = PythonOperator(
        task_id="store_config_in_dynamodb",
        python_callable=store_config_in_dynamodb,
    )

    discover_tables_to_archive = PythonOperator(
        task_id="discover_tables_to_archive",
        python_callable=invoke_discover_lambda,
    )

    prepare_args = PythonOperator(
        task_id="prepare_glue_job_args",
        python_callable=prepare_glue_job_args,
    )

    run_archive_jobs = run_glue_job.expand(
        script_args=prepare_args.output
    )

    run_validate_jobs = run_validate_job.expand(
        script_args=prepare_args.output
    )

    aggregate_job_metadata_task = PythonOperator(
    task_id="aggregate_job_metadata",
    python_callable=aggregate_job_metadata,
    )

    report_generation = PythonOperator(
    task_id="report_generation_lambda",
    python_callable=invoke_report_lambda,
    )

    print_raw_config >> validate_config >> discover_tables_to_archive >> store_config >> prepare_args >> run_archive_jobs >> run_validate_jobs >> aggregate_job_metadata_task >> report_generation   
