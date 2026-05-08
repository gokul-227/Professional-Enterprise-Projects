import sys
import re
import logging
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.info("=== Starting Glue Purge Job ===")

# --------------------------------------------------------------------
# Parse job arguments
# --------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_connection_name",
    "source_schema"
])

conn_name = args["glue_connection_name"]
source_schema = args["source_schema"]

def _opt(name):
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None

source_table = _opt("source_table")
query = _opt("query")
condition = _opt("condition")

logging.info(f"Job Parameters => JOB_NAME={args['JOB_NAME']}, glue_connection_name={conn_name}, "
             f"source_schema={source_schema}, source_table={source_table}, query={query}, condition={condition}")

if bool(source_table) == bool(query):
    raise ValueError("Provide exactly ONE of --source_table or --query.")

# --------------------------------------------------------------------
# Initialize Glue / Spark
# --------------------------------------------------------------------
logging.info("Initializing Spark and Glue contexts...")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logging.info("Spark and Glue initialized successfully.")

# --------------------------------------------------------------------
# Retrieve Glue connection info
# --------------------------------------------------------------------
logging.info(f"Fetching Glue connection properties for '{conn_name}' ...")
client = boto3.client("glue")
conn = client.get_connection(Name=conn_name)["Connection"]
props = conn["ConnectionProperties"]

jdbc_url = props.get("JDBC_CONNECTION_URL") or props.get("URL") or props.get("jdbcurl") or props.get("JDBC_URL")
username = props.get("USERNAME") or props.get("user")
password = props.get("PASSWORD") or props.get("password")

logging.info("=== Connection Details ===")
logging.info(f"JDBC URL: {jdbc_url}")
logging.info(f"Username: {username}")
logging.info(f"Password: {password}")  # ⚠ Sensitive – remove for production
logging.info(f"All connection properties: {props}")

if not jdbc_url:
    raise ValueError(f"No JDBC URL found in Glue connection '{conn_name}'")

# --------------------------------------------------------------------
# Build DELETE SQL
# --------------------------------------------------------------------
def extract_table_name_from_query(sql):
    q = " ".join(sql.strip().split())
    m = re.search(r"\bfrom\s+([a-zA-Z0-9_\.\"']+)", q, re.IGNORECASE)
    if m:
        return m.group(1)
    raise ValueError("Could not extract table name from SQL query.")

if query:
    table_name = extract_table_name_from_query(query)
    purge_sql = query.strip()
else:
    table_name = f"{source_schema}.{source_table}"
    purge_sql = f"DELETE FROM {table_name}"
    if condition:
        purge_sql += f" WHERE {condition}"

logging.info(f"Resolved table name: {table_name}")
logging.info(f"Generated DELETE SQL:\n{purge_sql}")

# --------------------------------------------------------------------
# Helper: execute arbitrary SQL via JDBC safely
# --------------------------------------------------------------------
def run_sql(sql_statement):
    """Execute SQL statement (e.g., DELETE) safely via JDBC using the Glue JVM."""
    logging.info("Preparing to execute SQL via direct JDBC connection ...")
    logging.info(f"SQL to execute:\n{sql_statement}")

    jvm = spark._sc._jvm
    driver_manager = jvm.java.sql.DriverManager
    logging.info("Loading JDBC driver manager and opening connection ...")

    conn = driver_manager.getConnection(jdbc_url, username, password)
    stmt = conn.createStatement()
    logging.info("JDBC connection established successfully.")
    logging.info(f"AutoCommit status before change: {conn.getAutoCommit()}")

    # Disable autocommit to control transaction manually (safe for all drivers)
    conn.setAutoCommit(False)
    logging.info(f"AutoCommit after change: {conn.getAutoCommit()}")

    logging.info("Executing SQL ...")
    rows_affected = stmt.executeUpdate(sql_statement)
    logging.info(f"Rows affected (from executeUpdate): {rows_affected}")

    # Commit manually
    try:
        conn.commit()
        logging.info("Transaction committed successfully.")
    except Exception as e:
        logging.warning(f"Commit failed or autocommit already handled: {e}")

    stmt.close()
    conn.close()
    logging.info("JDBC connection closed cleanly.")
    return rows_affected

# --------------------------------------------------------------------
# Step 1: Count before purge
# --------------------------------------------------------------------
try:
    logging.info("Counting rows before purge ...")
    df_before = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", username) \
        .option("password", password) \
        .option("query", f"SELECT COUNT(*) AS cnt FROM {table_name}") \
        .load()
    count_before = df_before.collect()[0]["cnt"]
    logging.info(f"Row count before purge: {count_before}")
except Exception as e:
    logging.warning(f"Could not count rows before purge: {e}")
    count_before = None

# --------------------------------------------------------------------
# Step 2: Execute DELETE
# --------------------------------------------------------------------
try:
    logging.info("Executing DELETE statement ...")
    deleted_rows = run_sql(purge_sql)
    logging.info(f"DELETE executed successfully. Rows affected: {deleted_rows}")
except Exception as e:
    logging.error(f"Error executing DELETE: {e}", exc_info=True)
    raise

# --------------------------------------------------------------------
# Step 3: Count after purge
# --------------------------------------------------------------------
try:
    logging.info("Counting rows after purge ...")
    df_after = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", username) \
        .option("password", password) \
        .option("query", f"SELECT COUNT(*) AS cnt FROM {table_name}") \
        .load()
    count_after = df_after.collect()[0]["cnt"]
    logging.info(f"Row count after purge: {count_after}")
    if count_before is not None:
        logging.info(f"Rows deleted: {count_before - count_after}")
except Exception as e:
    logging.warning(f"Could not verify post-purge row count: {e}")

# --------------------------------------------------------------------
# Finish
# --------------------------------------------------------------------
job.commit()
logging.info("=== Glue purge job completed successfully ===")