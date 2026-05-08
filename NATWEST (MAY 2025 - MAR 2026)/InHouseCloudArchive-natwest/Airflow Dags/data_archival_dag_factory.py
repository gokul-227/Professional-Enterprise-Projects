"""
natwest Data Archival Dynamic DAG Factory
-----------------------------------------
This file dynamically generates full DAGs (with all steps) based on YAML
configurations stored in S3.

Each YAML in s3://<bucket>/configs/ will create a fully functional DAG
with the same task structure as your base DAG.
"""

from __future__ import annotations
import uuid
import json
import pprint
import boto3
import yaml
import logging
from botocore.waiter import WaiterModel, create_waiter_with_client
from botocore.exceptions import WaiterError
from datetime import datetime
from airflow.models import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
S3_BUCKET = "bucket-eu-west-1-943277494469-daa-mwaa-dags-doc-mdo"      # e.g. mwaa-dev-dags
S3_PREFIX = "archival_configs/"
AWS_REGION = "eu-west-1"

DISCOVER_LAMBDA_FUNCTION_NAME = "natwest_data_archive_discover_tables"
ARCHIVE_GLUE_JOB_NAME = "natwest-data-archive-table-to-iceberg"
VALIDATE_GLUE_JOB_NAME = "natwest-archive-data-validation"
REPORT_GENERATION_LAMBDA_NAME = "natwest_data_archive_validation_report_generation"
ASSUME_ROLE_ARN = "arn:aws:iam::934336705194:role/DOC-Airflow-role-dev"
DYNAMODB_TABLE_NAME = "natwest-archival-configs"


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------
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
    Store final DAG run status and metadata in DynamoDB in flat key:value format,
    including flattened input_config_details.
    """
    print("--- Finalizing and storing DAG metadata in DynamoDB ---")
    table = ensure_dynamodb_table_exists()
    ti = context["ti"]

    dag_run_id = context["dag_run"].run_id
    run_uuid = str(uuid.uuid4())
    created_date = datetime.now().isoformat()

    # Fetch input config
    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    source = config["sources"][0]
    source_name = source["name"]
    env = config.get("env", "dev")

    # Extract triggered_by fields if available
    triggered_by_name = config.get("triggered_by", "unknown")
    triggered_by_email = config.get("email", "unknown")

    # Extract input config details (flattened)
    warehouse_key = source.get("warehouse", "")
    connection_key = source.get("connection", "")
    retention_policy_key = source.get("retention_policy", "")
    legal_hold_flag = source.get("legal_hold", False)

    # Also extract schema and table names from include -> schemas -> tables
    schema_names = []
    table_names = []
    include = source.get("include", {})
    schemas = include.get("schemas", [])
    for sch in schemas:
        sch_name = sch.get("name", "")
        if sch_name:
            schema_names.append(sch_name)
        tables = sch.get("tables", [])
        for t in tables:
            # Only when t has "name"
            table_name = t.get("name")
            if table_name:
                table_names.append(table_name)

    # Fetch job metadata
    job_metadata = ti.xcom_pull(task_ids="aggregate_job_metadata", key="job_metadata") or {}
    archive_jobs = job_metadata.get("archive_jobs", [])
    validate_jobs = job_metadata.get("validate_jobs", [])

    # Fetch report Lambda output
    lambda_raw = ti.xcom_pull(task_ids="report_generation_lambda", key="return_value")
    try:
        lambda_output = json.loads(lambda_raw) if lambda_raw else {}
        validation_report = lambda_output.get("validation_report", "")
        validation_summary = lambda_output.get("validation_summary_report", "")
    except Exception as e:
        print(f"Failed to parse Lambda output: {e}")
        validation_report = ""
        validation_summary = ""

    # Determine DAG status
    archive_status = all(job.get("status") == "SUCCEEDED" for job in archive_jobs)
    validate_status = all(job.get("status") == "SUCCEEDED" for job in validate_jobs)
    dag_status = "succeeded" if archive_status and validate_status else "failed"

    # Build item, flattened
    item = {
        "id": run_uuid,
        "source_name": source_name,
        "env": env,
        "airflow_run_id": dag_run_id,
        "created_date": created_date,
        "dag_status": dag_status,
        "triggered_by_name": triggered_by_name,
        "triggered_by_email": triggered_by_email,
        "validation_report_path": validation_report,
        "validation_summary_path": validation_summary,
        # input config details
        "input_warehouse": warehouse_key,
        "input_connection": connection_key,
        "input_retention_policy": retention_policy_key,
        "input_legal_hold": str(legal_hold_flag).lower(),
        "input_schema_names": ",".join(schema_names),
        "input_table_names": ",".join(table_names)
    }

    # Add flat archive job details
    for idx, job in enumerate(archive_jobs):
        prefix = f"archive_job_{idx}"
        item[f"{prefix}_job_run_id"] = job.get("job_run_id", "")
        item[f"{prefix}_status"] = job.get("status", "")
        item[f"{prefix}_start_time"] = job.get("start_time", "")
        item[f"{prefix}_end_time"] = job.get("end_time", "")
        item[f"{prefix}_duration"] = job.get("duration_seconds", "")
        # From script_args
        args = job.get("script_args", {})
        item[f"{prefix}_source_schema"] = args.get("--source_schema", "")
        item[f"{prefix}_source_table"] = args.get("--source_table", "")
        item[f"{prefix}_target_glue_table"] = args.get("--target_glue_table", "")

    # Add flat validate job details
    for idx, job in enumerate(validate_jobs):
        prefix = f"validate_job_{idx}"
        item[f"{prefix}_job_run_id"] = job.get("job_run_id", "")
        item[f"{prefix}_status"] = job.get("status", "")
        item[f"{prefix}_start_time"] = job.get("start_time", "")
        item[f"{prefix}_end_time"] = job.get("end_time", "")
        item[f"{prefix}_duration"] = job.get("duration_seconds", "")
        args = job.get("script_args", {})
        item[f"{prefix}_source_schema"] = args.get("--source_schema", "")
        item[f"{prefix}_source_table"] = args.get("--source_table", "")
        item[f"{prefix}_target_glue_table"] = args.get("--target_glue_table", "")

    # Store in DynamoDB
    try:
        table.put_item(Item=item)
        print("Successfully stored flattened metadata + input config in DynamoDB.")
    except Exception as e:
        raise AirflowException(f"Failed to store flattened metadata: {str(e)}")

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

    try:
        parsed = json.loads(raw)
        # Check if it's a wrapped API Gateway style response
        if "body" in parsed:
            lambda_output = json.loads(parsed["body"])
        else:
            lambda_output = parsed

        print("Parsed Lambda output:", json.dumps(lambda_output, indent=2))

        # Push cleaned output
        ti.xcom_push(key="return_value", value=json.dumps(lambda_output))
    except Exception as e:
        print(f"Failed to parse Lambda output: {e}")
        ti.xcom_push(key="return_value", value="{}")  # push empty JSON
    
    
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

# -------------------------------------------------------------------------
# STATIC VALIDATION (YAML SHAPE)
# -------------------------------------------------------------------------
def static_validate_config(conf: dict) -> dict:
    """Minimal static YAML validation/normalization."""
    must_have = ["dag_id", "schedule", "env", "connections", "warehouses", "retention_policies", "sources"]
    for k in must_have:
        if k not in conf:
            raise ValueError(f"YAML missing required key: {k}")

    # Normalize sources to list
    src = conf["sources"]
    if isinstance(src, dict):
        conf["sources"] = [src]
    elif not isinstance(src, list):
        raise ValueError("'sources' must be dict or list of dicts")
    return conf


# -------------------------------------------------------------------------
# DAG CREATOR
# -------------------------------------------------------------------------
"""
def create_archival_dag(dag_id, schedule, yaml_conf):
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    default_args = {"owner": "natwest", "start_date": datetime(2025, 1, 1)}
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
        tags=["archival", "dynamic"],
        description=f"Dynamically generated DAG from YAML ({dag_id})",
    )

    dag.default_args["conf"] = yaml_conf

    with dag:
        # --- For scheduled runs ---
        def load_conf_for_scheduled_run(**context):
            dag_run = context["dag_run"]
            if not dag_run.conf:
                dag_run.conf = yaml_conf
            return dag_run.conf

        ensure_conf = PythonOperator(
            task_id="ensure_conf_available",
            python_callable=load_conf_for_scheduled_run,
        )

        print_raw_config = PythonOperator(task_id="print_config", python_callable=print_config)
        validate_config = PythonOperator(task_id="parse_and_validate_config", python_callable=parse_and_validate_config)
        discover_tables_to_archive = PythonOperator(task_id="discover_tables_to_archive", python_callable=invoke_discover_lambda)
        prepare_args = PythonOperator(task_id="prepare_glue_job_args", python_callable=prepare_glue_job_args)

        #  FIX — run Glue jobs sequentially (no expand, no dynamic mapping)
        def run_archive_wrapper(**context):
            args_list = context["ti"].xcom_pull(task_ids="prepare_glue_job_args")
            if not args_list:
                raise AirflowException("No args prepared for archive Glue job.")
            results = []
            for args in args_list:
                results.append(run_glue_job(script_args=args))
            return results

        def run_validate_wrapper(**context):
            args_list = context["ti"].xcom_pull(task_ids="prepare_glue_job_args")
            if not args_list:
                raise AirflowException("No args prepared for validate Glue job.")
            results = []
            for args in args_list:
                results.append(run_validate_job(script_args=args))
            return results

        run_archive_jobs = PythonOperator(
            task_id="run_archive_glue_job",
            python_callable=run_archive_wrapper,
        )

        run_validate_jobs = PythonOperator(
            task_id="run_validate_glue_job",
            python_callable=run_validate_wrapper,
        )

        aggregate_job_metadata_task = PythonOperator(
            task_id="aggregate_job_metadata",
            python_callable=aggregate_job_metadata,
        )

        report_generation = PythonOperator(
            task_id="report_generation_lambda",
            python_callable=invoke_report_lambda,
        )

        store_config = PythonOperator(
            task_id="store_config_in_dynamodb",
            python_callable=store_config_in_dynamodb,
        )

        # --- Dependencies ---
        ensure_conf >> print_raw_config >> validate_config >> discover_tables_to_archive >> prepare_args
        prepare_args >> run_archive_jobs >> run_validate_jobs >> aggregate_job_metadata_task >> report_generation >> store_config

    return dag
"""  

import json
import logging
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

def run_archive_glue_job_runner(**context):
    """
    Pull list of arg dicts from XCom and invoke your existing TaskFlow
    function's underlying python via __wrapped__ to avoid PlainXComArg issues.
    """
    ti = context["ti"]
    args_list = ti.xcom_pull(task_ids="prepare_glue_job_args")

    # Defensive: normalize to list
    if args_list is None:
        raise AirflowException("prepare_glue_job_args returned None")
    if isinstance(args_list, str):
        try:
            args_list = json.loads(args_list)
        except Exception:
            raise AirflowException("prepare_glue_job_args returned a string that isn't JSON")
    if not isinstance(args_list, list):
        raise AirflowException(f"prepare_glue_job_args returned non-list: {type(args_list)}")

    results = []
    for i, args in enumerate(args_list, 1):
        if not isinstance(args, dict):
            logger.warning("Skipping non-dict args[%s]=%r", i, args)
            continue
        logger.info("Archive #%s starting with args: %s", i, json.dumps(args))
        # Call the ORIGINAL function body (no mapping, no XComArg)
        res = run_glue_job.__wrapped__(args)  # <-- Uses your existing logic
        logger.info("Archive #%s finished with result: %s", i, json.dumps(res, default=str))
        results.append(res)

    if not results:
        logger.warning("No archive jobs were started; args_list was empty.")
    return results


def run_validate_glue_job_runner(**context):
    """
    Same pattern as archive runner, for validation.
    """
    ti = context["ti"]
    args_list = ti.xcom_pull(task_ids="prepare_glue_job_args")

    if args_list is None:
        raise AirflowException("prepare_glue_job_args returned None")
    if isinstance(args_list, str):
        try:
            args_list = json.loads(args_list)
        except Exception:
            raise AirflowException("prepare_glue_job_args returned a string that isn't JSON")
    if not isinstance(args_list, list):
        raise AirflowException(f"prepare_glue_job_args returned non-list: {type(args_list)}")

    results = []
    for i, args in enumerate(args_list, 1):
        if not isinstance(args, dict):
            logger.warning("Skipping non-dict args[%s]=%r", i, args)
            continue
        logger.info("Validate #%s starting with args: %s", i, json.dumps(args))
        res = run_validate_job.__wrapped__(args)  # <-- Uses your existing logic
        logger.info("Validate #%s finished with result: %s", i, json.dumps(res, default=str))
        results.append(res)

    if not results:
        logger.warning("No validation jobs were started; args_list was empty.")
    return results




def create_archival_dag(dag_id, schedule, yaml_conf):
    """
    Dynamically create a single, fully functional DAG that:
      - Uses the YAML config directly for scheduled runs
      - Runs archive → validation Glue jobs sequentially
      - Reuses all helper functions from natwest_data_archival_to_iceberg.py
    """
    import logging
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    logger = logging.getLogger(__name__)
    logger.info(f"Creating DAG {dag_id} with schedule {schedule}")

    default_args = {
        "owner": "natwest",
        "start_date": datetime(2025, 1, 1),
        "retries": 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
        tags=["archival", "dynamic"],
        description=f"Dynamically generated archival DAG from YAML ({dag_id})",
    )

    # Store YAML config in default args for scheduled runs
    dag.default_args["conf"] = yaml_conf

    with dag:
        # -------------------------------------------------------
        # Inject YAML config for scheduled runs
        # -------------------------------------------------------
        def ensure_conf_available(**context):
            dag_run = context["dag_run"]
            if not dag_run.conf:
                dag_run.conf = yaml_conf
                logger.info("Injected YAML config into dag_run.conf for scheduled run.")
            return dag_run.conf

        ensure_conf = PythonOperator(
            task_id="ensure_conf_available",
            python_callable=ensure_conf_available,
        )

        # -------------------------------------------------------
        # Core DAG steps (reuse all your helper functions)
        # -------------------------------------------------------
        print_raw_config = PythonOperator(
            task_id="print_config",
            python_callable=print_config,
        )

        validate_config = PythonOperator(
            task_id="parse_and_validate_config",
            python_callable=parse_and_validate_config,
        )

        discover_tables = PythonOperator(
            task_id="discover_tables_to_archive",
            python_callable=invoke_discover_lambda,
        )

        prepare_args = PythonOperator(
            task_id="prepare_glue_job_args",
            python_callable=prepare_glue_job_args,
        )

        # ✅ Use wrappers instead of expand()
        run_archive_jobs = PythonOperator(
            task_id="run_archive_glue_job",
            python_callable=run_archive_glue_job_runner,
        )

        run_validate_jobs = PythonOperator(
            task_id="run_validate_glue_job",
            python_callable=run_validate_glue_job_runner,
        )

        aggregate_job_metadata_task = PythonOperator(
            task_id="aggregate_job_metadata",
            python_callable=aggregate_job_metadata,
        )

        report_generation = PythonOperator(
            task_id="report_generation_lambda",
            python_callable=invoke_report_lambda,
        )

        store_config = PythonOperator(
            task_id="store_config_in_dynamodb",
            python_callable=store_config_in_dynamodb,
        )

        # -------------------------------------------------------
        # Dependencies (exact same logical order)
        # -------------------------------------------------------
        ensure_conf >> print_raw_config >> validate_config >> discover_tables >> prepare_args
        prepare_args >> run_archive_jobs >> run_validate_jobs >> aggregate_job_metadata_task >> report_generation >> store_config

    logger.info(f"✅ DAG '{dag_id}' successfully created.")
    return dag
    
    
# -------------------------------------------------------------------------
# YAML LOADER AND DAG REGISTRATION
# -------------------------------------------------------------------------
def load_and_register_dag():
    """Load YAML configs from S3 and create one DAG per YAML."""
    s3 = boto3.client("s3", region_name=AWS_REGION)

    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        yaml_files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith((".yaml", ".yml"))]
        logger.info("Discovered %d YAML configs: %s", len(yaml_files), yaml_files)
    except Exception as e:
        logger.exception("Failed to list YAMLs from S3: %s", e)
        yaml_files = []

    for key in yaml_files:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            conf = yaml.safe_load(obj["Body"].read().decode("utf-8"))
            conf = static_validate_config(conf)

            dag_id = conf["dag_id"]
            schedule = conf["schedule"]

            dag = create_archival_dag(dag_id, schedule, conf)
            globals()[dag_id] = dag
            logger.info(" Registered DAG: %s (schedule=%s)", dag_id, schedule)

        except Exception as e:
            logger.exception(" Failed to register DAG from %s: %s", key, e)


# -------------------------------------------------------------------------
# EXECUTE LOADER
# -------------------------------------------------------------------------
load_and_register_dag()