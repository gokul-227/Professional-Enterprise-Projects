import sys
import re
from datetime import datetime, timedelta

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# ----------------------------
# Helpers
# ----------------------------
def parse_retention_duration(text: str) -> timedelta:
    m = re.match(r"^(\d+)([dmy])$", text.strip().lower())
    if not m:
        raise ValueError(f"Unsupported retention duration: {text}")
    num, unit = int(m[1]), m[2]
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])


# ----------------------------
# Job Parameters
# ----------------------------
args = getResolvedOptions(sys.argv, [
    "target_glue_db",
    "target_glue_table",
    "retention_policy_value",
    "legal_hold"
])

target_db = args["target_glue_db"]
target_table = args["target_glue_table"]
retention_value = args["retention_policy_value"]
legal_hold_flag = args["legal_hold"].lower() == "true"

# ----------------------------
# Spark / Glue Context
# ----------------------------
"""
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(f"update-retention-{target_table}", args)

# ----------------------------
# Configure Iceberg Catalog
# ----------------------------

warehouse_path = f"s3://{target_db}/iceberg/"  # Modify this if your Iceberg warehouse path is different
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_path)
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
"""
iceberg_warehouse = f"s3://natwest-data-archive-vault/iceberg/{target_db}/"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder \
    .appName("Iceberg Retention Policy Delete Job") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

job = Job(glueContext)


full_table_name = f"glue_catalog.{target_db}.{target_table}"

# ----------------------------
# Check if Glue Database and Table Exist
# ----------------------------
glue = boto3.client("glue")

try:
    glue.get_database(Name=target_db)
    print(f"[INFO] Glue database '{target_db}' exists.")
except glue.exceptions.EntityNotFoundException:
    raise Exception(f"[ERROR] Glue database '{target_db}' does not exist.")

try:
    glue.get_table(DatabaseName=target_db, Name=target_table)
    print(f"[INFO] Glue table '{target_table}' exists.")
except glue.exceptions.EntityNotFoundException:
    raise Exception(f"[ERROR] Glue table '{target_table}' does not exist in database '{target_db}'.")

# ----------------------------
# Compute Expiry Date
# ----------------------------
now = datetime.utcnow()
new_expiry = now + parse_retention_duration(retention_value)
expiry_str = new_expiry.strftime("%Y-%m-%d %H:%M:%S")

# ----------------------------
# Check Table Schema for Required Columns
# ----------------------------
print(f"[INFO] Checking if columns 'retention_expires_at' and 'legal_hold' exist...")
schema_df = spark.sql(f"DESCRIBE TABLE {full_table_name}")
columns = [row['col_name'] for row in schema_df.collect()]

required_columns = ["retention_expires_at", "legal_hold"]
for col in required_columns:
    if col not in columns:
        raise Exception(f"[ERROR] Column '{col}' not found in table '{full_table_name}'")

print("[INFO] Required columns are present. Proceeding with update.")

# ----------------------------
# Update All Rows
# ----------------------------
update_sql = f"""
    UPDATE {full_table_name}
    SET retention_expires_at = TIMESTAMP '{expiry_str}',
        legal_hold = {str(legal_hold_flag).lower()}
"""

print(f"[INFO] Executing UPDATE:\n{update_sql}")
spark.sql(update_sql)
print("[INFO] Retention policy update successful.")

job.commit()
print("[INFO] Glue job completed.")