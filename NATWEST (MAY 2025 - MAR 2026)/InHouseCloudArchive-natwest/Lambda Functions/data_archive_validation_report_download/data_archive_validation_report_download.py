import boto3
import base64
import urllib.parse
import os
import json

s3 = boto3.client("s3")
athena = boto3.client("athena")

ATHENA_DB = "validation_reports"
ATHENA_OUTPUT = "s3://natwest-data-archive-assets/athena_query_history/"
#TABLE_NAME = "validation_reports"

def lambda_handler(event, context):
    print("Lambda Triggered with Event : ", json.dumps(event))
    params = event.get("queryStringParameters") or {}
    report_id = params.get("report_id")
    report_type = params.get("report_type")
    table_param = params.get("table")
    print(f"Extracted Parameters: report_id={report_id}, report_type={report_type}, table={table_param}")

    if not report_id or not table_param or report_type not in ("validation_report", "validation_summary_report"):
        return {
            "statusCode": 400,
            "body": "Invalid report_type or report_id or table"
        }
    TABLE_NAME = f"{table_param}_validation_report"
    print(f"Constructed Table Name: {TABLE_NAME}")
    query = f"SELECT {report_type} FROM {TABLE_NAME} WHERE id = '{report_id}' LIMIT 1"
    print(f" Athena Query: {query}")
    
    try:
        execution = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
        )
    except Exception as e:
        print(f"Failed to start Athena query: {str(e)}")
        return {"statusCode": 500, "body": "Failed to start Athena query"}

    exec_id = execution["QueryExecutionId"]
    print(f" Athena QueryExecutionId: {exec_id}")
    
    # Poll for query completion
    while True:
        response = athena.get_query_execution(QueryExecutionId=exec_id)
        state = response["QueryExecution"]["Status"]["State"]
        print(f" Athena Query State: {state}")
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if state != "SUCCEEDED":
        print("Athena query did not succeed.")
        return {"statusCode": 500, "body": "Athena query failed"}

    results = athena.get_query_results(QueryExecutionId=exec_id)
    rows = results["ResultSet"]["Rows"]
    print(f"Query Result Rows Count: {len(rows)}")
    print("Full Athena Result :", json.dumps(rows, indent =2))

    if len(rows) == 0 :
        print("No result found for the given report_id.")
        return {"statusCode": 404, "body": "No result"}

    s3_url = rows[1]["Data"][0]["VarCharValue"]
    """
    if len(rows) < 1 or 'Data' not in rows[0] or len(rows[0]['Data']) == 0:
        print(" No data row found in Athena result")
        return{"Status code" : 404, "Body" : "No data found in Athena"}
    
    data_row = rows[0] if len(rows) == 1 else row[1]
    s3_url = data_row['Data'][0]["VarCharValue"]
    """
    print(f" S3 URL Retrieved from Athena: {s3_url}")
    parsed = urllib.parse.urlparse(s3_url)
    bucket = parsed.netloc.split(".s3")[0]
    key = parsed.path.lstrip("/")
    print(f"S3 Bucket: {bucket}, Key: {key}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        filename = os.path.basename(key)
        print(f"File downloaded: {filename}, Size: {len(content)} bytes")
    except Exception as e:
        print(f"Failed to fetch or read S3 object: {str(e)}")
        return {"statusCode": 500, "body": "Failed to retrieve report from S3"}

    # Return the file as a base64-encoded response
    print("Returning PDF file in response.")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/pdf",
            "Content-Disposition": f"attachment; filename={filename}"
        },
        "body": base64.b64encode(content).decode("utf-8"),
        "isBase64Encoded": True
    }
