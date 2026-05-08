import sys
import json
import boto3
from datetime import datetime, date
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function for Glue job that handles both discovery and delete modes
    """
    # Get basic arguments first
    basic_args = getResolvedOptions(sys.argv, [
        'mode',
        'source_name', 
        'connection',
        'iceberg_database'
    ])
    
    mode = basic_args['mode']
    source_name = basic_args['source_name']
    connection = basic_args['connection']
    iceberg_database = basic_args['iceberg_database']
    
    # Get max_parallel_tables only for delete mode
    max_parallel_tables = 5  # Default value
    if mode == 'delete':
        try:
            delete_args = getResolvedOptions(sys.argv, ['max_parallel_tables'])
            max_parallel_tables = int(delete_args['max_parallel_tables'])
        except:
            logger.warning("max_parallel_tables not provided for delete mode, using default: 5")
    
    logger.info(f"Starting Glue job in {mode} mode")
    logger.info(f"Source: {source_name}")
    logger.info(f"Connection: {connection}")
    logger.info(f"Iceberg Database: {iceberg_database}")
    if mode == 'delete':
        logger.info(f"Max Parallel Tables: {max_parallel_tables}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"NatwestDataPurge-{mode}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://natwest-data-archive-vault/iceberg/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()
    
    try:
        if mode == 'discovery':
            discovery_results = run_discovery_mode(spark, iceberg_database, source_name)
            
        elif mode == 'delete':
            purge_results = run_delete_mode(spark, iceberg_database, source_name, max_parallel_tables)
            logger.info(f"Purge completed. Results: {purge_results}")
            
        else:
            raise ValueError(f"Unknown mode: {mode}. Supported modes: discovery, delete")
            
    except Exception as e:
        logger.error(f"Error in {mode} mode: {str(e)}")
        raise
    finally:
        spark.stop()

def run_discovery_mode(spark, iceberg_database, source_name):
    """
    Discovery mode: Find all tables with expired data
    """
    logger.info(f"Starting discovery mode for database: {iceberg_database}")
    
    try:
        # Get all tables in the iceberg database
        tables_df = spark.sql(f"SHOW TABLES IN glue_catalog.{iceberg_database}")
        table_names = [row['tableName'] for row in tables_df.collect()]
        
        logger.info(f"Found {len(table_names)} tables in database {iceberg_database}")
        
        tables_with_expired_data = []
        total_estimated_deletes = 0
        
        for table_name in table_names:
            logger.info(f"Checking table: {table_name}")
            
            try:
                # Check if table has the required columns
                table_schema = spark.sql(f"DESCRIBE glue_catalog.{iceberg_database}.{table_name}").collect()
                column_names = [row['col_name'] for row in table_schema]
                
                if 'retention_expires_at' not in column_names or 'legal_hold' not in column_names:
                    logger.warning(f"Table {table_name} missing required columns (retention_expires_at, legal_hold). Skipping.")
                    continue
                
                # Count expired rows
                count_query = f"""
                SELECT COUNT(*) as expired_count
                FROM glue_catalog.{iceberg_database}.{table_name}
                WHERE retention_expires_at < current_date()
                AND legal_hold = false
                """
                
                result = spark.sql(count_query).collect()
                expired_count = result[0]['expired_count']
                
                if expired_count > 0:
                    table_info = {
                        "iceberg_schema": iceberg_database,
                        "table_name": table_name,
                        "estimated_rows_to_delete": expired_count
                    }
                    tables_with_expired_data.append(table_info)
                    total_estimated_deletes += expired_count
                    
                    logger.info(f"Table {table_name} has {expired_count} rows to purge")
                else:
                    logger.info(f"Table {table_name} has no expired data")
                    
            except Exception as e:
                logger.error(f"Error checking table {table_name}: {str(e)}")
                continue
        
        discovery_results = {
            "tables_with_data_to_purge": tables_with_expired_data,
            "total_estimated_deletes": total_estimated_deletes,
            "discovery_timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # Save to S3 for delete job to read later
        save_discovery_results(discovery_results, source_name)
        
        # Log detailed summary for user review
        logger.info("=== DISCOVERY SUMMARY ===")
        logger.info(f"Tables scanned: {len(table_names)}")
        logger.info(f"Tables with expired data: {len(tables_with_expired_data)}")
        logger.info(f"Total estimated rows to delete: {total_estimated_deletes}")
        
        if tables_with_expired_data:
            logger.info("Tables requiring purge:")
            for table in tables_with_expired_data:
                logger.info(f"  - {table['table_name']}: {table['estimated_rows_to_delete']} rows")
        else:
            logger.info("No tables require purging - all data is within retention period")
        
        logger.info("=== END DISCOVERY SUMMARY ===")
        
        return discovery_results
        
    except Exception as e:
        logger.error(f"Error in discovery mode: {str(e)}")
        raise

def run_delete_mode(spark, iceberg_database, source_name, max_parallel_tables):
    """
    Delete mode: Purge expired data from tables
    """
    logger.info(f"Starting delete mode for database: {iceberg_database}")
    
    # Load discovery results from S3
    discovery_results = load_discovery_results(source_name)
    tables_to_purge = discovery_results.get('tables_with_data_to_purge', [])
    
    if not tables_to_purge:
        logger.info("No tables to purge found in discovery results")
        return {"status": "completed", "tables_processed": 0}
    
    logger.info(f"Will purge {len(tables_to_purge)} tables with max parallelism: {max_parallel_tables}")
    
    # Use Spark parallelism for table deletions
    def delete_table_data(table_info):
        table_name = table_info['table_name']
        estimated_rows = table_info['estimated_rows_to_delete']
        
        try:
            logger.info(f"Starting deletion for table: {table_name} (estimated {estimated_rows} rows)")
            
            delete_query = f"""
            DELETE FROM glue_catalog.{iceberg_database}.{table_name}
            WHERE retention_expires_at < current_date()
            AND legal_hold = false
            """
            
            # Execute delete
            spark.sql(delete_query)
            
            # Get actual count after deletion (for verification)
            verify_query = f"""
            SELECT COUNT(*) as remaining_expired_count
            FROM glue_catalog.{iceberg_database}.{table_name}
            WHERE retention_expires_at < current_date()
            AND legal_hold = false
            """
            
            remaining_result = spark.sql(verify_query).collect()
            remaining_count = remaining_result[0]['remaining_expired_count']
            
            result = {
                "table": table_name,
                "status": "success",
                "estimated_rows_deleted": estimated_rows,
                "remaining_expired_rows": remaining_count,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully purged table {table_name}. Remaining expired rows: {remaining_count}")
            return result
            
        except Exception as e:
            error_result = {
                "table": table_name,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            logger.error(f"Failed to purge table {table_name}: {str(e)}")
            return error_result

    deletion_results = []
    # Use ThreadPoolExecutor to run deletions in parallel on the driver
    with ThreadPoolExecutor(max_workers=max_parallel_tables) as executor:
        future_to_table = {executor.submit(delete_table_data, table): table for table in tables_to_purge}
        
        for future in as_completed(future_to_table):
            deletion_results.append(future.result())
    
    # Summarize results
    successful_deletions = [r for r in deletion_results if r['status'] == 'success']
    failed_deletions = [r for r in deletion_results if r['status'] == 'failed']
    
    summary = {
        "status": "completed",
        "tables_processed": len(deletion_results),
        "successful_deletions": len(successful_deletions),
        "failed_deletions": len(failed_deletions),
        "results": deletion_results
    }
    
    logger.info("=== PURGE SUMMARY ===")
    logger.info(f"Tables processed: {len(deletion_results)}")
    logger.info(f"Successful deletions: {len(successful_deletions)}")
    logger.info(f"Failed deletions: {len(failed_deletions)}")
    
    if successful_deletions:
        logger.info("Successfully purged tables:")
        for result in successful_deletions:
            logger.info(f"  - {result['table']}: {result['estimated_rows_deleted']} rows deleted")
    
    if failed_deletions:
        logger.error("Failed to purge tables:")
        for result in failed_deletions:
            logger.error(f"  - {result['table']}: {result['error']}")
    
    logger.info("=== END PURGE SUMMARY ===")
    
    return summary

def save_discovery_results(discovery_results, source_name):
    """Save discovery results to S3 for later use by delete mode"""
    try:
        s3_client = boto3.client('s3')
        bucket = "natwest-data-archive-vault"
        key = f"purge-temp/{source_name}/discovery-results.json"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(discovery_results, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Discovery results saved to s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Error saving discovery results: {str(e)}")
        raise

def load_discovery_results(source_name):
    """Load discovery results from S3"""
    try:
        s3_client = boto3.client('s3')
        bucket = "natwest-data-archive-vault"
        key = f"purge-temp/{source_name}/discovery-results.json"
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        discovery_results = json.loads(response['Body'].read().decode('utf-8'))
        
        logger.info(f"Discovery results loaded from s3://{bucket}/{key}")
        return discovery_results
        
    except Exception as e:
        logger.error(f"Error loading discovery results: {str(e)}")
        raise

if __name__ == "__main__":
    main()
