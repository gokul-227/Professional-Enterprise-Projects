import sys
import re
import json
import uuid
from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pyspark.sql.functions import col, md5, concat_ws, coalesce, lit
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructType, StructField, Row
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def add_checksum_column(df: DataFrame) -> DataFrame:
    """
    Adds a 'checksum' column using MD5 hash of all columns, safely handling nulls and data types.
    """
    # Replace nulls with 'NULL' as string, and cast all columns to string
    string_cols = [coalesce(col(c).cast("string"), lit("NULL")) for c in df.columns]
    
    return df.withColumn("checksum", md5(concat_ws("||", *string_cols)))
    
def create_glue_database_if_not_exists(db_name: str, region: str = "eu-west-1"):
    """Creates a Glue database if it doesn't already exist."""
    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Glue database '{db_name}' not found. Creating it...")
            try:
                glue.create_database(DatabaseInput={"Name": db_name})
                print(f"Glue database '{db_name}' created.")
            except ClientError as create_error:
                if create_error.response["Error"]["Code"] == "AlreadyExistsException":
                    print(f"Glue database '{db_name}' was created by another job.")
                else:
                    raise
        else:
            raise

def _parse_retention_duration(text: str) -> timedelta:
    """Parses a simple duration string like '30d', '6m', or '1y'."""
    m = re.match(r"^(\d+)([dmy])$", text, re.I)
    if not m:
        raise ValueError(f"Unsupported duration literal: {text!r}")
    num, unit = int(m[1]), m[2].lower()
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])

def upload_blob_to_s3(binary_data, path):
    """Uploads a binary object to a given S3 path."""
    s3 = boto3.client("s3")
    parsed = urlparse(path)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    s3.put_object(Bucket=bucket, Key=key, Body=binary_data)
    print(f"Uploaded blob to s3://{bucket}/{key}")

def get_extension_from_content(data: bytes) -> str:
    """
    Inspects the first few bytes to guess a file extension; falls back to .bin.
    """
    if not data:
        return ".bin"
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.lstrip().startswith(b"<?xml") or data.lstrip().startswith(b"<"):
        return ".xml"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith(b"%PDF-"):
        return ".pdf"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return ".gif"
    return ".bin"

# --------------------------------------------------------------------------- #
# Arg parsing (table/query optional; XOR enforced)
# --------------------------------------------------------------------------- #

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_schema",
        "glue_connection_name",
        "target_s3_path",
        "target_glue_db",
        "target_glue_table",
        "retention_policy_value",
        "legal_hold",
    ],
)

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
if condition:
    print(f"Condition (pushdown): {condition}")

# --------------------------------------------------------------------------- #
# Initialize Spark/Glue context
# --------------------------------------------------------------------------- #
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

parsed = urlparse(args["target_s3_path"])
warehouse_root = f"s3://{parsed.netloc}/iceberg/"

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_root)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# --------------------------------------------------------------------------- #
# Read from source — DataFrame JDBC using Glue Connection properties
# (supports inline SQL and pushdown for condition)
# --------------------------------------------------------------------------- #

def _to_tcps_descriptor(simple_url: str, dn: str | None) -> str:
    """
    Convert 'jdbc:oracle:thin:@//HOST:PORT/SERVICE' to a TCPS descriptor with DN,
    else return unchanged if already a descriptor.
    """
    if "(DESCRIPTION=" in simple_url or "(ADDRESS=" in simple_url:
        return simple_url  # already descriptor
    m = re.match(r"^jdbc:oracle:thin:@//([^:/]+):(\d+)/(.*)$", simple_url)
    if not m:
        return simple_url
    host, port, service = m.groups()
    sec = f"(SECURITY=(ssl_server_cert_dn=\"{dn}\"))" if dn else ""
    return (
        "jdbc:oracle:thin:@"
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={host})(PORT={port}))"
        f"(CONNECT_DATA=(SERVICE_NAME={service})){sec})"
    )

# -------------------- NEW: DB kind detection -------------------- #
def _db_kind_from_url(jdbc_url: str) -> str:
    """
    Minimal classifier so we can adjust driver and URL safely.
    """
    u = jdbc_url.lower()
    if u.startswith("jdbc:oracle:"):
        return "oracle"
    if u.startswith("jdbc:postgresql:"):
        return "postgres"
    return "unknown"
# ---------------------------------------------------------------- #

# 1) Pull connection details
glue = boto3.client("glue")
sm = boto3.client("secretsmanager")

conn = glue.get_connection(Name=args["glue_connection_name"])["Connection"]
p = conn["ConnectionProperties"]

jdbc_url_in   = p["JDBC_CONNECTION_URL"]
secret_arn    = p.get("SECRET_ID")
cert_dn       = p.get("CUSTOM_JDBC_CERT_STRING")
enforce_ssl   = (p.get("JDBC_ENFORCE_SSL", "false").lower() == "true")
driver_class  = p.get("JDBC_DRIVER_CLASS_NAME", None)  # allow auto-pick below

# Determine DB kind so we can pick sensible defaults
db_kind = _db_kind_from_url(jdbc_url_in)
if not driver_class:
    driver_class = "oracle.jdbc.OracleDriver" if db_kind == "oracle" else "org.postgresql.Driver"

# 2) Resolve credentials
if not secret_arn:
    raise ValueError("Glue connection is missing SECRET_ID for DB credentials.")
creds = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
jdbc_user, jdbc_password = creds["username"], creds["password"]

# 3) SSL / URL handling
# Truststore is provided at job level via Extra file + spark.*.extraJavaOptions.
jdbc_url = jdbc_url_in

if db_kind == "oracle":
    # Upgrade thin URL to TCPS descriptor when SSL is enforced
    jdbc_url = _to_tcps_descriptor(jdbc_url_in, cert_dn if enforce_ssl else None)
elif db_kind == "postgres":
    # If SSL is enforced and URL lacks sslmode, append the usual bits
    if enforce_ssl and ("sslmode=" not in jdbc_url_in.lower()):
        sep = "&" if "?" in jdbc_url_in else "?"
        jdbc_url = jdbc_url_in + f"{sep}ssl=true&sslmode=require"

print(f"[DB={db_kind}] Using JDBC URL: {jdbc_url[:200]}{'...' if len(jdbc_url) > 200 else ''}")
print(f"[DB={db_kind}] Driver class: {driver_class}")

# 4) Build the DataFrame reader (supports inline SQL dbtable)
reader = (
    spark.read.format("jdbc")
         .option("url", jdbc_url)
         .option("driver", driver_class)
         .option("user", jdbc_user)
         .option("password", jdbc_password)
         .option("fetchsize", "10000")
)

# Keep your Oracle optimization; add a small PG tweak
if db_kind == "oracle":
    reader = reader.option("oracle.jdbc.defaultLobPrefetchSize", "104857600")
elif db_kind == "postgres":
    # Helps with some type inference edge cases
    reader = reader.option("stringtype", "unspecified")

schema_name = args["source_schema"]

if query:
    dbtable = f"({query.strip()}) t"
elif condition:
    dbtable = f"(SELECT * FROM {schema_name}.{source_table} WHERE {condition}) t"
else:
    dbtable = f"{schema_name}.{source_table}"

print(f"JDBC dbtable used: {dbtable[:300]}{'...' if len(dbtable)>300 else ''}")
df = reader.option("dbtable", dbtable).load()

row_count = df.count()
print(f"Read {row_count} rows from {schema_name}.{source_table or '[inline SQL]'}")
if row_count == 0:
    print("No rows to process. Exiting.")
    job.commit()
    sys.exit(0)

# --------------------------------------------------------------------------- #
# Extract and Upload Blob Columns
# --------------------------------------------------------------------------- #
blob_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, BinaryType)]
print(f"Binary columns detected: {blob_columns}")

# Schema for the DataFrame after blob processing
#schema_fields = [
#    StructField(field.name, field.dataType) for field in df.schema.fields
#    if not isinstance(field.dataType, BinaryType)
#]
schema_fields = [StructField(field.name, field.dataType) for field in df.schema.fields]
schema_fields.extend([StructField(f"{col}_s3_path", StringType(), True) for col in blob_columns])
schema_after_blobs = StructType(schema_fields)

# Choose a stable segment for S3 paths (table name or hashed query)
def _safe_segment():
    if source_table:
        return source_table.lower()
    import hashlib
    return ("query_" + hashlib.sha1(query.encode("utf-8")).hexdigest()[:8]).lower()

safe_segment = _safe_segment()

def handle_row(row):
    """Processes a single row, uploading blobs and returning a transformed Row object."""
    row_dict = row.asDict()
    #result_dict = {k: v for k, v in row_dict.items() if k not in blob_columns}
    result_dict = row_dict.copy()

    for col in blob_columns:
        data = row_dict.get(col)
        if data and len(data) > 0:
            blob_uuid = str(uuid.uuid4())
            s3_path = (
                f"s3://{parsed.netloc}/blobs/"
                f"{args['target_glue_db'].lower()}/{schema_name.lower()}/"
                f"{safe_segment}/{col.lower()}/{blob_uuid}"
            )
            ext = get_extension_from_content(data)
            if ext:
                s3_path = s3_path + ext
            upload_blob_to_s3(data, s3_path)
            result_dict[f"{col}_s3_path"] = s3_path
        else:
            result_dict[f"{col}_s3_path"] = None

    ordered_values = [result_dict.get(field.name) for field in schema_after_blobs.fields]
    return Row(*ordered_values)

if blob_columns:
    rdd = df.rdd.map(handle_row)
    df = spark.createDataFrame(rdd, schema=schema_after_blobs)

# --------------------------------------------------------------------------- #
# Add retention metadata columns
# --------------------------------------------------------------------------- #
legal_hold = args["legal_hold"].lower() == "true"
now = datetime.utcnow()
expires = now + _parse_retention_duration(args["retention_policy_value"])

df = (
    df.withColumn("archived_at", F.lit(now))
      .withColumn("retention_expires_at", F.lit(expires))
      .withColumn("legal_hold", F.lit(legal_hold))
)
df = add_checksum_column(df)
# --------------------------------------------------------------------------- #
# Write to Iceberg table
# --------------------------------------------------------------------------- #
create_glue_database_if_not_exists(args["target_glue_db"])

iceberg_table = f"glue_catalog.{args['target_glue_db']}.{args['target_glue_table'].lower()}"
print(f"Writing to Iceberg table: {iceberg_table}")

# Keeping your original behavior:
df.writeTo(iceberg_table).using("iceberg").createOrReplace()

print("Archive complete.")
job.commit()
