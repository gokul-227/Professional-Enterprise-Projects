import sys
import re
import json
import hashlib
import decimal
import logging
from awsglue.transforms import *
from pyspark.sql.window import Window
from typing import List, Dict, Optional
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, lower, trim, when, date_format, format_number, concat_ws, md5, collect_list


# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------------
# Parse arguments
# ---------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'glue_connection_name',
    'source_schema',
    'target_glue_db',
    'target_glue_table'
])
#condition = sys.argv[sys.argv.index("--condition") + 1] if "--condition" in sys.argv else None
conn_name = args['glue_connection_name']
source_schema = args['source_schema']
iceberg_db = args['target_glue_db']
iceberg_table = args['target_glue_table']



logging.info(f"Starting job: {args['JOB_NAME']}")
def extract_table_name_from_query(query: str) -> str:
    # Clean and flatten the query
    query_clean = " ".join(query.strip().split())

    # Regex to find the first FROM <table_name>
    match = re.search(r'\bfrom\s+([a-zA-Z0-9_\.]+)', query_clean, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        raise ValueError("Could not extract table name from the SQL query.")

def _opt(name: str):
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None

source_table = _opt("source_table")   # OPTIONAL
query = _opt("query")                 # OPTIONAL (inline SQL)
condition = _opt("condition")         # OPTIONAL (server-side pushdown for table path)

# XOR enforcement: exactly one of table or query
if bool(source_table) == bool(query):
    raise ValueError("Provide exactly ONE of --source_table or --query.")

print(
    f"Starting archival for schema: {args['source_schema']} -> "
    + (f"table={source_table}" if source_table else "query=<<inline SQL>>")
)
table_name = f"{source_schema}.{source_table}"

if condition:
    print(f"Condition (pushdown): {condition}")
if query:
    table_name = extract_table_name_from_query(query)
    print("table_name extracted from query:", table_name)
    


# ---------------------------
# Initialize Spark and Glue contexts (DO NOT recreate SparkSession)
# ---------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder \
    .appName("Iceberg Retention Policy Delete Job") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.info("Spark and Glue contexts initialized")

# ---------------------------
# Functions
# ---------------------------

def read_source_table(connection_name: str, table_name: str) -> DataFrame:
    logging.info(f"Reading source table '{table_name}' from connection '{connection_name}'")
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": table_name,
            "connectionName": connection_name
        }
    )
    df = dyf.toDF()
    if condition:
        logging.info(f"Applying filter condition on source: {condition}")
        df = df.filter(condition)
    if query:
        temp_view_name = "temp_table"
        df.createOrReplaceTempView(temp_view_name)
        logging.info(f"Created temp view '{temp_view_name}' for the table data")

        # Replace the original table name in query with temp view name
        modified_query = re.sub(
            r'from\s+' + re.escape(table_name),
            f'from {temp_view_name}',
            query,
            flags=re.IGNORECASE
        )
        logging.info(f"Modified query to run on temp view:\n{modified_query}")

        # Run the SQL query on Spark temp view
        df = spark.sql(modified_query)
    df = df.select([col(c).alias(c.strip().lower()) for c in df.columns])
    logging.info(f"Source DataFrame has {df.count()} rows, columns: {df.columns}")
    df.printSchema()
    df.show()
    return df


def read_target_table(db: str, table: str) -> DataFrame:
    full_table = f"glue_catalog.{db}.{table}".lower()
    logging.info(f"Reading target Iceberg table: {full_table}")
    df = spark.sql(f"SELECT * FROM {full_table}")
    
    if condition:
        logging.info(f"Applying filter condition on target: {condition}")
        df = df.filter(condition)
    if query :
        # Replace the original table name in query with temp view name
        modified_query = re.sub(
            r'from\s+' + re.escape(table_name),
            f'from {full_table}',
            query,
            flags=re.IGNORECASE
        )
        logging.info(f"Modified query to run on temp view:\n{modified_query}")

        # Run the SQL query on Spark temp view
        df = spark.sql(modified_query)
    df = df.select([col(c).alias(c.strip().lower()) for c in df.columns])
    logging.info(f"Target DataFrame has {df.count()} rows, columns: {df.columns}")
    df.printSchema()
    df.show()
    return df


def normalize_dataframe(df: DataFrame, ignore_cols: List[str]) -> DataFrame:
    logging.info("Starting dataframe normalization")
    for c in df.columns:
        if c in ignore_cols:
            continue
        dtype = str(df.schema[c].dataType).lower()
        try:
            if "date" in dtype or "timestamp" in dtype:
                df = df.withColumn(c, when(col(c).isNull(), lit("null"))
                                   .otherwise(date_format(col(c), "yyyy-MM-dd HH:mm:ss")))
            elif any(t in dtype for t in ["decimal", "double", "float"]):
                df = df.withColumn(c, when(col(c).isNull(), lit("null"))
                                   .otherwise(format_number(col(c).cast("double"), 5)))
            elif any(t in dtype for t in ["int", "bigint", "smallint"]):
                df = df.withColumn(c, when(col(c).isNull(), lit("null"))
                                   .otherwise(col(c).cast("string")))
            elif "boolean" in dtype:
                df = df.withColumn(c, when(col(c).isNull(), lit("null"))
                                   .otherwise(when(col(c) == True, lit("true")).otherwise(lit("false"))))
            else:
                df = df.withColumn(c, when(col(c).isNull(), lit("null"))
                                   .otherwise(trim(lower(col(c).cast("string")))))
        except Exception as e:
            logging.warning(f"Normalization skipped for column {c} due to error: {e}")
    logging.info("Dataframe normalization complete")
    return df
    
def validate_row_count(source_df: DataFrame, target_df: DataFrame) -> dict:
    logging.info("Starting row count validation")
    src_count = source_df.count()
    tgt_count = target_df.count()
    diff = abs(src_count - tgt_count)

    result = {
        "result": "passed" if src_count == tgt_count else "failed",
        "source_row_count": src_count,
        "target_row_count": tgt_count,
        "difference": diff
    }

    logging.info(f"Row count validation result: {result['result']} | Source: {src_count} | Target: {tgt_count} | Difference: {diff}")
    return result

def validate_schema(source_df: DataFrame, target_df: DataFrame, ignore_cols: List[str]) -> dict:
    logging.info("Starting schema validation")
    src_cols = sorted([c for c in source_df.columns if c not in ignore_cols])
    tgt_cols = sorted([c for c in target_df.columns if c not in ignore_cols])
    diff = list(set(src_cols).symmetric_difference(set(tgt_cols)))
    result = {
        "result": "passed" if not diff else "failed",
        "source_columns": src_cols,
        "target_columns": tgt_cols,
        "difference": diff
    }
    logging.info(f"Schema validation result: {result['result']} | Differences: {diff}")
    return result

def validate_column_checksums(source_df: DataFrame, target_df: DataFrame, columns: List[str]) -> dict:
    logging.info("Starting column-level checksum validation")

    def col_checksum(df: DataFrame, col_name: str) -> str:
        agg_df = df.select(col_name) \
            .na.fill('null') \
            .orderBy(col_name) \
            .agg(md5(concat_ws("", collect_list(col_name))).alias("checksum"))
        checksum = agg_df.collect()[0]["checksum"]
        return checksum

    src_chksum = {}
    tgt_chksum = {}

    for col_name in columns:
        src_checksum = col_checksum(source_df, col_name)
        tgt_checksum = col_checksum(target_df, col_name)

        src_chksum[col_name] = src_checksum
        tgt_chksum[col_name] = tgt_checksum

    # FULL LOGGING — uncomment only if needed
    logging.info("Source column checksums:")
    for col, checksum in src_chksum.items():
        logging.info(f"{col}: {checksum}")

    logging.info("Target column checksums:")
    for col, checksum in tgt_chksum.items():
        logging.info(f"{col}: {checksum}")

    mismatched = [col for col in columns if src_chksum[col] != tgt_chksum[col]]

    return {
        "result": "passed" if not mismatched else "failed",
        "source": src_chksum,
        "target": tgt_chksum,
        "mismatched_columns": mismatched
    }


from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import md5, concat_ws, col
import logging

def validate_row_checksums(
    source_df: DataFrame,
    target_df: DataFrame,
    columns: List[str],
    check_columns: List[str] = None
) -> dict:
    logging.info("Starting row-level checksum validation")
    
    cols_to_check = check_columns if check_columns else columns
    
    src = source_df.select(*columns).withColumn(
        "row_checksum", md5(concat_ws("|", *[col(c) for c in cols_to_check]))
    )
    tgt = target_df.select(*columns).withColumn(
        "row_checksum", md5(concat_ws("|", *[col(c) for c in cols_to_check]))
    )

    # Log up to 100 row checksums for source
    src_checksums = src.select("row_checksum").limit(100).collect()
    logging.info(f"Source row checksums (up to 100): Count={len(src_checksums)}")
    for row in src_checksums:
        logging.info(f"Source Checksum: {row['row_checksum']}")

    # Log up to 100 row checksums for target
    tgt_checksums = tgt.select("row_checksum").limit(100).collect()
    logging.info(f"Target row checksums (up to 100): Count={len(tgt_checksums)}")
    for row in tgt_checksums:
        logging.info(f"Target Checksum: {row['row_checksum']}")

    # Rows only in source (not in target)
    src_only = src.join(
        tgt.select(*columns, "row_checksum"),
        on=columns,
        how="left_anti"
    )
    # Rows only in target (not in source)
    tgt_only = tgt.join(
        src.select(*columns, "row_checksum"),
        on=columns,
        how="left_anti"
    )

    def row_key(row):
        return tuple(row[c] for c in columns)

    src_dict = {
        row_key(row): {
            "record": {c: row[c] for c in cols_to_check},
            "checksum": row["row_checksum"]
        }
        for row in src_only.collect()
    }
    tgt_dict = {
        row_key(row): {
            "record": {c: row[c] for c in cols_to_check},
            "checksum": row["row_checksum"]
        }
        for row in tgt_only.collect()
    }

    all_keys = sorted(set(src_dict.keys()).union(set(tgt_dict.keys())))

    not_matching_records = []
    for key in all_keys:
        pair = {}
        if key in src_dict:
            pair["source_row"] = src_dict[key]
        if key in tgt_dict:
            pair["target_row"] = tgt_dict[key]
        not_matching_records.append(pair)

    mismatch_count = (src_only.count() + tgt_only.count()) // 2

    total_count = source_df.count()
    mismatch_percent = round((mismatch_count / total_count) * 100, 2) if total_count else 0.0

    logging.info(f"Row checksum mismatch count: {mismatch_count} of {total_count} rows ({mismatch_percent}%)")
    
    # Log full details of all not matching records
    logging.info(f"Full details of not matching records (count: {len(not_matching_records)}):")
    for rec in not_matching_records:
        logging.info(rec)

    return {
        "result": "passed" if mismatch_count == 0 else "failed",
        "row_count": total_count,
        "mismatch_count": mismatch_count,
        "mismatch_percent": mismatch_percent,
        "not_matching_records": not_matching_records
    }


def write_validation_summary(result: dict, source_table: str, target_table: str, output_path: str) -> None:
    from pyspark.sql import Row

    rows = []

    # Schema Validation
    schema_val = result['schema_validation']
    schema_value = f"source_columns: {schema_val['source_columns']}, target_columns: {schema_val['target_columns']}"
    schema_remarks = "Mismatched columns: " + ", ".join(schema_val['difference']) if schema_val['result'] == 'failed' else ""
    rows.append(Row(
        validation_report="schema_validation",
        source_table=source_table,
        target_table=target_table,
        result=schema_val['result'],
        value=schema_value,
        remarks=schema_remarks
    ))

    # Row Count Validation
    row_count_val = result['row_count']
    diff_count = abs(row_count_val['source_row_count'] - row_count_val['target_row_count'])
    row_count_value = f"source_table_count: {row_count_val['source_row_count']}, target_table_count: {row_count_val['target_row_count']}"
    row_count_remarks = f"Mismatched row count. Difference: {row_count_val['difference']}" if row_count_val['result'] == 'failed' else ""
    rows.append(Row(
        validation_report="row_count_validation",
        source_table=source_table,
        target_table=target_table,
        result=row_count_val['result'],
        value=row_count_value,
        remarks=row_count_remarks
    ))

    # Row Checksum Validation
    row_checksum_val = result['row_checksum']
    row_checksum_value = f"row checksums validated: {row_checksum_val['row_count']}"
    not_matching_str = str(row_checksum_val['not_matching_records']) if row_checksum_val['result'] == 'failed' else ""
    row_checksum_remarks = f"Mismatched checksums: {row_checksum_val['mismatch_count']} rows ({row_checksum_val['mismatch_percent']}%). Records: {not_matching_str}" if row_checksum_val['result'] == 'failed' else ""
    rows.append(Row(
        validation_report="row_checksum_validation",
        source_table=source_table,
        target_table=target_table,
        result=row_checksum_val['result'],
        value=row_checksum_value,
        remarks=row_checksum_remarks
    ))

    # Column Checksum Validation
    col_checksum_val = result['column_checksum']
    src_col_checksum = [f"{k}: {v}" for k, v in col_checksum_val['source'].items()]
    tgt_col_checksum = [f"{k}: {v}" for k, v in col_checksum_val['target'].items()]
    col_checksum_value = f"source_column_checksum: [{', '.join(src_col_checksum)}], target_column_checksum: [{', '.join(tgt_col_checksum)}]"
    col_checksum_remarks = "Mismatched columns: " + ", ".join(col_checksum_val['mismatched_columns']) if col_checksum_val['result'] == 'failed' else "Passed"
    rows.append(Row(
        validation_report="column_checksum_validation",
        source_table=source_table,
        target_table=target_table,
        result=col_checksum_val['result'],
        value=col_checksum_value,
        remarks=col_checksum_remarks
    ))

    summary_df = spark.createDataFrame(rows)

    # Write CSV
    summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path + "csv/")

    # Write JSON
    summary_df.coalesce(1).write.mode("overwrite").json(output_path + "json/")

    logging.info(f"Validation summary written to: {output_path}")
    logging.info(f"Validation JSON written to: {output_path}/json")




# ---------------------------
# Main Logic
# ---------------------------

source_df = read_source_table(conn_name, table_name)
target_df = read_target_table(iceberg_db, iceberg_table.lower())

# Detect ignored columns (present in target but not in source)
ignore_columns = list(set(target_df.columns) - set(source_df.columns))
logging.info(f"Ignoring columns: {ignore_columns}")

# Normalize both
source_df = normalize_dataframe(source_df, ignore_columns)
target_df = normalize_dataframe(target_df, ignore_columns)

# Get common columns
common_columns = [c for c in source_df.columns if c not in ignore_columns]

# Run validations
result = {
    "schema_validation": validate_schema(source_df, target_df, ignore_columns),
    "row_count": validate_row_count(source_df, target_df),
    "column_checksum": validate_column_checksums(source_df, target_df, common_columns),
    "row_checksum": validate_row_checksums(source_df, target_df, common_columns, validate_column_checksums(source_df, target_df, common_columns)["mismatched_columns"])
}

# Log final result (summary only)
logging.info("Complete Validation Result:")
logging.info(json.dumps(result, indent=4))

output_path = f"s3://natwest-data-archive-vault/validation_report/{iceberg_db}/{iceberg_table}/validation_summary/"
write_validation_summary(result, table_name, f"{iceberg_db}.{iceberg_table}", output_path)


job.commit()
