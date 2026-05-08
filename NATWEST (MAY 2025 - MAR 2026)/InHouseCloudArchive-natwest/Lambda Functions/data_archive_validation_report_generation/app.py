import boto3
import json
import re
import ast
import time
import logging
from io import BytesIO
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client('s3')
BUCKET = 'natwest-data-archive-vault'
BASE_PREFIX = 'validation_report'

# Styles
styles = getSampleStyleSheet()
title_style = ParagraphStyle(
    'CenteredTitle',
    parent=styles['Heading1'],
    alignment=1,
    fontSize=18,
    spaceAfter=20
)
section_style = ParagraphStyle(
    'SectionHeading',
    parent=styles['Heading2'],
    spaceBefore=12,
    spaceAfter=6
)
normal_style = ParagraphStyle(
    'NormalText',
    parent=styles['BodyText'],
    fontSize=10,
    leading=12
)

def header_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    canvas.drawString(40, 25, "Validation Report - Confidential")
    canvas.drawRightString(570, 25, f"Page {doc.page}")
    canvas.restoreState()

def empty_footer(canvas, doc):
    pass

def horizontal_line(width=500):
    line = Table([[""]], colWidths=[width])
    line.setStyle(TableStyle([('LINEBELOW', (0, 0), (-1, -1), 1, colors.black)]))
    return line

def style_table(tbl):
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2F4F4F")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F9F9F9")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F5F5F5")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
    ]))
    return tbl

def highlight_result(val):
    val = str(val).upper()
    if val in ("PASSED", "SUCCEEDED"):
        return f'<font color="green"><b>{val}</b></font>'
    if val in ("FAILED", "ERROR"):
        return f'<font color="red"><b>{val}</b></font>'
    return val

def row_count_validation(story, rc):
    logger.info("Running row_count_validation...")
    story.append(Paragraph("ROW COUNT VALIDATION", section_style))
    if rc:
        m = re.search(r"source_table_count: (\d+), target_table_count: (\d+)", rc["value"])
        if m:
            s, t = map(int, m.groups())
            logger.info(f"Row count source: {s}, target: {t}")
            diff = abs(s - t)
            tbl = Table([
                ["Source Count", "Target Count", "Difference", "Result"],
                [s, t, diff, Paragraph(highlight_result(rc["result"]), normal_style)]
            ], colWidths=[120]*4)
            style_table(tbl)
            story.append(tbl)
    story.append(Spacer(1, 12))

def schema_validation(story, sc):
    logger.info("Running schema_validation...")
    story.append(Paragraph("SCHEMA VALIDATION", section_style))
    if sc:
        m = re.search(r"source_columns: \[(.*?)\], target_columns: \[(.*?)\]", sc["value"])
        if m:
            src_cols = [c.strip("' ") for c in m.group(1).split(",")]
            tgt_cols = [c.strip("' ") for c in m.group(2).split(",")]
            logger.info(f"Source columns: {src_cols}")
            logger.info(f"Target columns: {tgt_cols}")
            all_cols = sorted(set(src_cols) | set(tgt_cols))
            data = [["Source Col", "Target Col", "Difference", "Result"]]
            for c in all_cols:
                diff = "-" if c in src_cols and c in tgt_cols else c
                res = "PASSED" if c in src_cols and c in tgt_cols else "FAILED"
                data.append([
                    c if c in src_cols else "",
                    c if c in tgt_cols else "",
                    diff,
                    Paragraph(highlight_result(res), normal_style)
                ])
            tbl = Table(data, colWidths=[140, 140, 140, 80])
            style_table(tbl)
            story.append(tbl)
    story.append(Spacer(1, 12))

def column_checksum_validation(story, cc):
    logger.info("Running column_checksum_validation...")
    story.append(Paragraph("COLUMN CHECKSUM VALIDATION", section_style))
    if cc and "value" in cc:
        src_cs, tgt_cs = {}, {}
        sm = re.search(r"source_column_checksum: \[(.*?)\]", cc["value"])
        tm = re.search(r"target_column_checksum: \[(.*?)\]", cc["value"])
        if sm:
            for it in sm.group(1).split(", "):
                if ":" in it:
                    c, v = it.split(":", 1)
                    src_cs[c.strip()] = v.strip()
        if tm:
            for it in tm.group(1).split(", "):
                if ":" in it:
                    c, v = it.split(":", 1)
                    tgt_cs[c.strip()] = v.strip()
        logger.info(f"Source checksums: {src_cs}")
        logger.info(f"Target checksums: {tgt_cs}")
        cols = sorted(set(src_cs) | set(tgt_cs))
        data = [["Source Column", "Target Column", "Mismatch", "Result"]]
        for c in cols:
            s_val = src_cs.get(c, "N/A")
            t_val = tgt_cs.get(c, "N/A")
            mismatch = c if s_val != t_val else "-"
            res = "FAILED" if s_val != t_val else "PASSED"
            data.append([c, c, mismatch, Paragraph(highlight_result(res), normal_style)])
            data.append([s_val, t_val, "" if mismatch == "-" else mismatch, ""])
        tbl = Table(data, colWidths=[140, 140, 140, 80])
        style_table(tbl)
        story.append(tbl)
    story.append(Spacer(1, 12))

def row_checksum_validation(story, rcsh):
    logger.info("Running row_checksum_validation...")
    story.append(Paragraph("ROW CHECKSUM VALIDATION", section_style))
    if rcsh:
        remarks = rcsh.get("remarks", "")
        m_records = re.search(r"Records:\s*(\[.*\])", remarks)
        record_list = []
        if m_records:
            try:
                record_list = ast.literal_eval(m_records.group(1))
            except Exception as e:
                logger.error(f"Error parsing row mismatch records: {e}")
                story.append(Paragraph(f"Error parsing row mismatches: {e}", normal_style))

        m_validated = re.search(r"row checksums validated: (\d+)", rcsh.get("value", ""))
        m_mismatch = re.search(r"Mismatched checksums: (\d+) rows \(([\d.]+)%\)", remarks)

        validated_count = int(m_validated.group(1)) if m_validated else 0
        mismatch_count = int(m_mismatch.group(1)) if m_mismatch else len(record_list)
        mismatch_percent = float(m_mismatch.group(2)) if m_mismatch else 0.0
        result = "FAILED" if mismatch_count > 0 else "PASSED"

        logger.info(f"Row checksum validated: {validated_count}, mismatches: {mismatch_count}, %: {mismatch_percent}")

        summary_data = [
            ["ROW CHECKSUM VALIDATED COUNT", "ROW CHECKSUM MISMATCH COUNT", "ROW CHECKSUM MISMATCH %", "RESULT"],
            [validated_count, mismatch_count, f"{mismatch_percent}%", Paragraph(highlight_result(result), normal_style)],
        ]
        summary_table = Table(summary_data, colWidths=[160, 160, 160, 80])
        style_table(summary_table)
        story.append(summary_table)
        story.append(Spacer(1, 12))

        if record_list:
            story.append(Paragraph("ROW CHECKSUM MISMATCH RECORDS", section_style))
            for i in range(0, len(record_list), 2):
                src_data, tgt_data = {}, {}
                rec1 = record_list[i]
                if "source_row" in rec1:
                    src_data = rec1["source_row"]
                elif "target_row" in rec1:
                    tgt_data = rec1["target_row"]
                if i + 1 < len(record_list):
                    rec2 = record_list[i + 1]
                    if "source_row" in rec2:
                        src_data = rec2["source_row"]
                    elif "target_row" in rec2:
                        tgt_data = rec2["target_row"]

                src_r = src_data.get("record", {})
                tgt_r = tgt_data.get("record", {})
                s_ck = src_data.get("checksum", "")
                t_ck = tgt_data.get("checksum", "")
                cols = sorted(set(src_r.keys()) | set(tgt_r.keys()))
                rows = [["COLUMN", "SOURCE ROW VALUE", "TARGET ROW VALUE"]]
                for c in cols:
                    rows.append([c, str(src_r.get(c, "")), str(tgt_r.get(c, ""))])
                rows.append(["CHECKSUM", s_ck, t_ck])

                tbl = Table(rows, colWidths=[90, 235, 235])
                style_table(tbl)
                story.append(tbl)
                story.append(Spacer(1, 6))
    else:
        logger.warning("No row checksum data found.")
        story.append(Paragraph(" No row checksum data found.", normal_style))

    story.append(Spacer(1, 24))
    story.append(horizontal_line())
    story.append(Spacer(1, 12))

def load_validation_records(target_glue_db, target_glue_table):
    logger.info(f"Loading validation records for {target_glue_db}.{target_glue_table}")
    prefix = f"{BASE_PREFIX}/{target_glue_db}/{target_glue_table}/validation_summary/json/"
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            logger.info(f"Reading S3 object: {key}")
            if key.endswith(".json"):
                obj_data = s3.get_object(Bucket=BUCKET, Key=key)
                body = obj_data["Body"].read().decode("utf-8")
                for line in body.strip().split("\n"):
                    try:
                        records.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping malformed line in {key}")
    return records

def get_job_details(jobs, target_table, key="--target_glue_table"):
    logger.info(f"Filtering job details for table: {target_table}")
    return [job for job in jobs if job["script_args"].get(key) == target_table]

def group_by_validation(records):
    logger.info("Grouping validation records by type")
    return {r["validation_report"]: r for r in records}

def process_table(story, src_table, tgt_db, tgt_table, val_recs, arch_jobs, val_jobs):
    logger.info(f"Processing validation for {src_table} → {tgt_db}.{tgt_table}")
    story.append(PageBreak())
    story.append(horizontal_line())
    story.append(Spacer(1, 6))
    story.append(Paragraph("VALIDATION REPORT", title_style))
    story.append(Paragraph(f"<b>SOURCE TABLE:</b> {src_table}", normal_style))
    story.append(Paragraph(f"<b>TARGET TABLE:</b> {tgt_db}.{tgt_table}", normal_style))
    story.append(Spacer(1, 12))

    story.append(Paragraph("ARCHIVAL JOB", section_style))
    for job in arch_jobs:
        logger.info(f"Archival job: {job['job_name']}")
        story.append(Paragraph(f"<b>JOB NAME:</b> {job['job_name']}", normal_style))
        story.append(Paragraph(f"<b>JOB RUN ID:</b> {job['job_run_id']}", normal_style))
        story.append(Paragraph(f"<b>JOB RUN TIME (SEC):</b> {job['duration_seconds']}", normal_style))
        story.append(Paragraph(f"<b>JOB STATUS:</b> {highlight_result(job['status'])}", normal_style))
        story.append(Spacer(1, 6))

    story.append(Paragraph("VALIDATION JOB", section_style))
    for job in val_jobs:
        logger.info(f"Validation job: {job['job_name']}")
        story.append(Paragraph(f"<b>JOB NAME:</b> {job['job_name']}", normal_style))
        story.append(Paragraph(f"<b>JOB RUN ID:</b> {job['job_run_id']}", normal_style))
        story.append(Paragraph(f"<b>JOB RUN TIME (SEC):</b> {job['duration_seconds']}", normal_style))
        story.append(Paragraph(f"<b>JOB STATUS:</b> {highlight_result(job['status'])}", normal_style))
        story.append(Spacer(1, 6))

    validations = group_by_validation(val_recs)
    row_count_validation(story, validations.get("row_count_validation"))
    schema_validation(story, validations.get("schema_validation"))
    column_checksum_validation(story, validations.get("column_checksum_validation"))
    row_checksum_validation(story, validations.get("row_checksum_validation"))

def generate_failure_report(run_id, timestamp, arch_jobs_all, val_jobs_all):
    target_database = val_jobs_all[0]["script_args"]["--target_glue_db"]
    story = []

    # Cover Page
    story.append(Spacer(1, 200))
    story.append(Paragraph("DATA VALIDATION REPORT", title_style))
    story.append(Paragraph(f"Run ID: {run_id}", normal_style))
    story.append(Paragraph(f"Timestamp: {timestamp}", normal_style))
    story.append(PageBreak())

    for job in val_jobs_all:
        #tgt_table = job["script_args"]["--target_glue_table"]
        #tgt_db = job["script_args"]["--target_glue_db"]
        #src_schema = job["script_args"]["--source_schema"]
        #src_table = job["script_args"]["--source_table"]
        #full_src_table = f"{src_schema}.{src_table}"

        script_args = job.get("script_args", {})
        tgt_table = script_args.get("--target_glue_table")
        tgt_db = script_args.get("--target_glue_db")  # assuming typo?
        src_schema = script_args.get("--source_schema")
        src_table = script_args.get("--source_table") or script_args.get("--query", "UNKNOWN_SOURCE")
        full_src_table = f"{src_schema}.{src_table}"

        val_records = load_validation_records(tgt_db, tgt_table)
        if not val_records:
            continue

        validations = group_by_validation(val_records)

        row_count = validations.get("row_count_validation")
        schema = validations.get("schema_validation")
        col_checksum = validations.get("column_checksum_validation")
        row_checksum = validations.get("row_checksum_validation")

        # Collect results
        def is_failed(val):
            return val and val["result"].upper() not in ("PASSED", "SUCCEEDED")

        failed_any = any([
            is_failed(row_count),
            is_failed(schema),
            is_failed(col_checksum),
            is_failed(row_checksum)
        ])

        # Always print the summary (even if no failures)
        story.append(Paragraph(f"<b>SOURCE TABLE:</b> {full_src_table}", normal_style))
        story.append(Paragraph(f"<b>TARGET TABLE:</b> {tgt_db}.{tgt_table}", normal_style))
        story.append(Spacer(1, 6))

        # Create summary block table
                # Combined 1-row summary table with all validations
        summary_table = [
            [
                "ROW COUNT VALIDATION",
                "SCHEMA VALIDATION",
                "COLUMN CHECKSUM VALIDATION",
                "ROW CHECKSUM VALIDATION"
            ],
            [
                Paragraph(highlight_result(row_count["result"]) if row_count else "-", normal_style),
                Paragraph(highlight_result(schema["result"]) if schema else "-", normal_style),
                Paragraph(highlight_result(col_checksum["result"]) if col_checksum else "-", normal_style),
                Paragraph(highlight_result(row_checksum["result"]) if row_checksum else "-", normal_style),
            ]
        ]
        tbl = Table(summary_table, colWidths=[135, 135, 160, 130])
        style_table(tbl)
        story.append(tbl)
        story.append(Spacer(1, 12))


        if failed_any:
            # Add failed validation details
            if is_failed(row_count):
                row_count_validation(story, row_count)

            if is_failed(schema):
                story.append(Paragraph("SCHEMA VALIDATION - FAILED COLUMNS", section_style))
                m = re.search(r"source_columns: \[(.*?)\], target_columns: \[(.*?)\]", schema["value"])
                if m:
                    src_cols = [c.strip("' ") for c in m.group(1).split(",")]
                    tgt_cols = [c.strip("' ") for c in m.group(2).split(",")]
                    all_cols = sorted(set(src_cols) | set(tgt_cols))
                    data = [["Source Col", "Target Col", "Difference", "Result"]]
                    for c in all_cols:
                        if c not in src_cols or c not in tgt_cols:
                            diff = c
                            res = "FAILED"
                            data.append([
                                c if c in src_cols else "",
                                c if c in tgt_cols else "",
                                diff,
                                Paragraph(highlight_result(res), normal_style)
                            ])
                    tbl = Table(data, colWidths=[140, 140, 140, 80])
                    style_table(tbl)
                    story.append(tbl)
                    story.append(Spacer(1, 12))

            if is_failed(col_checksum):
                story.append(Paragraph("COLUMN CHECKSUM VALIDATION - FAILED COLUMNS", section_style))
                src_cs, tgt_cs = {}, {}
                sm = re.search(r"source_column_checksum: \[(.*?)\]", col_checksum["value"])
                tm = re.search(r"target_column_checksum: \[(.*?)\]", col_checksum["value"])
                if sm:
                    for it in sm.group(1).split(", "):
                        if ":" in it:
                            c, v = it.split(":", 1)
                            src_cs[c.strip()] = v.strip()
                if tm:
                    for it in tm.group(1).split(", "):
                        if ":" in it:
                            c, v = it.split(":", 1)
                            tgt_cs[c.strip()] = v.strip()
                cols = sorted(set(src_cs) | set(tgt_cs))
                data = [["Source Column", "Target Column", "Mismatch", "Result"]]
                for c in cols:
                    s_val = src_cs.get(c, "N/A")
                    t_val = tgt_cs.get(c, "N/A")
                    if s_val != t_val:
                        mismatch = c
                        res = "FAILED"
                        data.append([c, c, mismatch, Paragraph(highlight_result(res), normal_style)])
                        data.append([s_val, t_val, "", ""])
                tbl = Table(data, colWidths=[140, 140, 140, 80])
                style_table(tbl)
                story.append(tbl)
                story.append(Spacer(1, 12))

            if is_failed(row_checksum):
                row_checksum_validation(story, row_checksum)

        # Line between table blocks
        story.append(horizontal_line())
        story.append(Spacer(1, 12))

    # Upload if any tables were processed
    if len(story) > 1:
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
        doc.build(story, onFirstPage=empty_footer, onLaterPages=header_footer)

        output_key = f"validation_consolidated_report/{target_database}/{timestamp}/{run_id}/validation_final_report.pdf"
        pdf_buffer.seek(0)
        s3.put_object(Body=pdf_buffer, Bucket=BUCKET, Key=output_key, ContentType='application/pdf')

        print(f" Failure report uploaded to s3://{BUCKET}/{output_key}")

def wait_for_athena_query(athena_client, query_execution_id):
    """Waits for Athena query to complete and returns final state."""
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state
        time.sleep(2)

def insert_report_metadata_into_athena(id, run_id, timestamp, report_key, failure_report_key, target_database, source_schema, source_tables):
    athena_client = boto3.client('athena')
    athena_database = 'validation_reports'  # Fixed database
    table_name = f"{target_database}_validation_report"

    output_location = 's3://natwest-data-archive-assets/athena_query_history/'

    # S3 URL construction
    bucket = BUCKET
    region = 'eu-west-1'
    report_url = f"https://{bucket}.s3.{region}.amazonaws.com/{report_key}"
    failure_report_url = f"https://{bucket}.s3.{region}.amazonaws.com/{failure_report_key}"
    created_date = timestamp.split('T')[0]

    # Check/Create Table (in validation_reports DB)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {athena_database}.{table_name} (
        id string,
        airflow_run_id STRING,
        application_name STRING,
        source_schema STRING,
        source_tables STRING,
        validation_summary_report STRING,
        validation_report STRING,
        created_date DATE
    )
    LOCATION 's3://natwest-data-archive-vault/iceberg/{athena_database}/{table_name}/'
    TBLPROPERTIES ('table_type'='ICEBERG')
    """

    create_resp = athena_client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={'Database': athena_database},
        ResultConfiguration={'OutputLocation': output_location}
    )

    create_exec_id = create_resp['QueryExecutionId']
    print(f"CREATE TABLE started: Execution ID = {create_exec_id}")
    create_state = wait_for_athena_query(athena_client, create_exec_id)

    if create_state != "SUCCEEDED":
        print(f"CREATE TABLE failed with state: {create_state}")
        return

    print(f"Table {athena_database}.{table_name} is ready.")

    # 2Insert metadata
    insert_query = f"""
    INSERT INTO {athena_database}.{table_name} (
        id,
        airflow_run_id,
        application_name,
        source_schema,
        source_tables,
        validation_summary_report,
        validation_report,
        created_date
    )
    VALUES (
        '{id}',
        '{run_id}',
        '{target_database}',
        '{source_schema}',
        '{",".join(source_tables)}',
        '{report_url}',
        '{failure_report_url}',
        DATE('{created_date}')
    )
    """

    insert_resp = athena_client.start_query_execution(
        QueryString=insert_query,
        QueryExecutionContext={'Database': athena_database},
        ResultConfiguration={'OutputLocation': output_location}
    )

    insert_exec_id = insert_resp['QueryExecutionId']
    print(f"INSERT started: Execution ID = {insert_exec_id}")
    insert_state = wait_for_athena_query(athena_client, insert_exec_id)

    if insert_state == "SUCCEEDED":
        print(f"Record inserted into {athena_database}.{table_name}")
    else:
        print(f" INSERT failed with state: {insert_state}")


def lambda_handler(event, context):
    logger.info("Lambda handler invoked.")
    logger.info("Received event: %s", json.dumps(event))
    print("Received event: %s", json.dumps(event))
    id = event.get("run_id")
    run_id = event['metadata']['airflow_run_id']
    timestamp = event.get("timestamp")
    metadata = event.get("metadata", {})
    arch_jobs_all = metadata.get("archive_jobs", [])
    val_jobs_all = metadata.get("validate_jobs", [])

    story = []

    story.append(Spacer(1, 200))
    story.append(Paragraph("DATA VALIDATION SUMMARY REPORT", title_style))
    story.append(Paragraph(f"Run ID: {run_id}", normal_style))
    story.append(Paragraph(f"Timestamp: {timestamp}", normal_style))

    for job in val_jobs_all:
        script_args = job.get("script_args", {})
        tgt_table = script_args.get("--target_glue_table")
        tgt_db = script_args.get("--target_glue_db")  # assuming typo?
        src_schema = script_args.get("--source_schema")
        src_table = script_args.get("--source_table") or script_args.get("--query", "UNKNOWN_SOURCE")
        full_src_table = f"{src_schema}.{src_table}"
        logger.info(f"Handling validation for {full_src_table} → {tgt_db}.{tgt_table}")

        val_records = load_validation_records(tgt_db, tgt_table)
        if not val_records:
            logger.warning(f"No validation data found for table: {tgt_table}")
            story.append(PageBreak())
            story.append(horizontal_line())
            story.append(Spacer(1, 6))
            story.append(Paragraph("VALIDATION REPORT", title_style))
            story.append(Paragraph(f"<b>SOURCE TABLE:</b> {full_src_table}", normal_style))
            story.append(Paragraph(f"<b>TARGET TABLE:</b> {tgt_db}.{tgt_table}", normal_style))
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"No validation data found for table '{tgt_table}'.", normal_style))
            story.append(Spacer(1, 24))
            story.append(horizontal_line())
            story.append(Spacer(1, 12))
            continue

        table_arch_jobs = get_job_details(arch_jobs_all, tgt_table)
        table_val_jobs = get_job_details(val_jobs_all, tgt_table)

        process_table(story, full_src_table, tgt_db, tgt_table, val_records, table_arch_jobs, table_val_jobs)

    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
    doc.build(story, onFirstPage=empty_footer, onLaterPages=header_footer)

    target_database = val_jobs_all[0]["script_args"]["--target_glue_db"]
    source_schema = val_jobs_all[0]["script_args"]["--source_schema"]
    #source_tables = [job["script_args"]["--source_table"] for job in val_jobs_all]
    source_tables = [job["script_args"].get("--source_table") for job in val_jobs_all if "--source_table" in job["script_args"]]

    output_key = f"validation_consolidated_summary_report/{target_database}/{timestamp}/{run_id}/validation_final_summary_report.pdf"
    logger.info(f"Uploading PDF report to s3://{BUCKET}/{output_key}")
    pdf_buffer.seek(0)
    s3.put_object(Body=pdf_buffer, Bucket=BUCKET, Key=output_key, ContentType='application/pdf')
    generate_failure_report(run_id, timestamp, arch_jobs_all, val_jobs_all)

    report_key = f"validation_consolidated_summary_report/{target_database}/{timestamp}/{run_id}/validation_final_summary_report.pdf"
    failure_report_key = f"validation_consolidated_report/{target_database}/{timestamp}/{run_id}/validation_final_report.pdf"

    insert_report_metadata_into_athena(
        id,
        run_id,
        timestamp,
        report_key,
        failure_report_key,
        target_database,
        source_schema,
        source_tables
    )
    """
    return {
        "statusCode": 200,
        "body": f"PDF report uploaded to s3://{BUCKET}/{output_key}"
    }
    """
    return {
        "validation_report": f"s3://{BUCKET}/validation_consolidated_report/{target_database}/{timestamp}/{run_id}/validation_final_report.pdf",
        "validation_summary_report": f"s3://{BUCKET}/validation_consolidated_summary_report/{target_database}/{timestamp}/{run_id}/validation_final_summary_report.pdf"
    }