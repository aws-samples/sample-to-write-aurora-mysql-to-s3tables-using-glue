import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONNECTION_NAME', 'DATABASE_NAME', 'S3_TABLE_NAMESPACE'])

# Initialize Spark context
sc = SparkContext()

# Configure Spark session for S3 Tables REST endpoint
spark = SparkSession.builder.appName("MySQLToS3TablesREST") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3_rest_catalog") \
    .config("spark.sql.catalog.s3_rest_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3_rest_catalog.type", "rest") \
    .config("spark.sql.catalog.s3_rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.s3_rest_catalog.rest-metrics-reporting-enabled", "false") \
    .config('spark.sql.parquet.enableVectorizedReader', 'false') \
    .getOrCreate()

# Initialize Glue context and job
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Spark session initialized for S3 Tables migration with REST endpoint")

# Configuration - read from job parameters (set by CloudFormation)
mysql_connection = args['CONNECTION_NAME']
database = args['DATABASE_NAME']
s3_table_namespace = args['S3_TABLE_NAMESPACE']

# Define TICKIT tables to migrate from MySQL
tables_config = {
    "users": {
        "primary_key": "userid",
        "description": "User information"
    },
    "venue": {
        "primary_key": "venueid",
        "description": "Venue details"
    },
    "category": {
        "primary_key": "catid",
        "description": "Event categories"
    },
    "date": {
        "primary_key": "dateid",
        "description": "Date dimension"
    },
    "event": {
        "primary_key": "eventid",
        "description": "Event information"
    },
    "listing": {
        "primary_key": "listid",
        "description": "Ticket listings"
    },
    "sales": {
        "primary_key": "salesid",
        "description": "Sales transactions"
    }
}

def read_mysql_table(table_name):
    """Read table from Aurora MySQL using JDBC connection"""
    try:
        logger.info(f"Reading table: {table_name} from Aurora MySQL")

        # Create dynamic frame from MySQL using the Glue connection
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": table_name,
                "connectionName": mysql_connection,
            },
            transformation_ctx=f"read_mysql_{table_name}"
        )

        record_count = dynamic_frame.count()
        logger.info(f"Successfully read {record_count} records from {table_name}")

        if record_count == 0:
            logger.warning(f"Table {table_name} is empty")

        return dynamic_frame

    except Exception as e:
        logger.error(f"Error reading table {table_name} from MySQL: {str(e)}")
        return None

def write_to_s3_tables_iceberg(dynamic_frame, table_name):
    """Write data to S3 Tables using Iceberg format via REST endpoint"""
    try:
        if dynamic_frame is None:
            logger.warning(f"No data frame provided for table {table_name}")
            return False

        record_count = dynamic_frame.count()
        if record_count == 0:
            logger.warning(f"No data to write for table {table_name}")
            return True

        # Convert to Spark DataFrame for Iceberg operations
        df = dynamic_frame.toDF()

        # Log schema information
        logger.info(f"Schema for {table_name}:")
        df.printSchema()

        # Create namespace if it doesn't exist
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {s3_table_namespace}")
            logger.info(f"Namespace {s3_table_namespace} ready")
        except Exception as e:
            logger.warning(f"Namespace creation warning (may already exist): {str(e)}")

        # Full table name in S3 Tables
        full_table_name = f"{s3_table_namespace}.{table_name}"

        logger.info(f"Writing {record_count} records to S3 Tables: {full_table_name}")

        # Register DataFrame as temporary view
        df.createOrReplaceTempView(f"temp_{table_name}")

        # Check if table exists
        table_exists = False
        try:
            spark.sql(f"DESCRIBE TABLE {full_table_name}")
            table_exists = True
            logger.info(f"Table {full_table_name} already exists")
        except Exception as e:
            logger.info(f"Table {full_table_name} does not exist, will create it")

        if table_exists:
            # Use INSERT OVERWRITE to replace all data (Iceberg supported)
            logger.info(f"Using INSERT OVERWRITE to replace data in {full_table_name}")
            insert_sql = f"INSERT OVERWRITE {full_table_name} SELECT * FROM temp_{table_name}"
            spark.sql(insert_sql)
        else:
            # Create table with schema and insert data
            columns_ddl = ", ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {columns_ddl}
            ) USING ICEBERG
            """
            spark.sql(create_table_sql)
            logger.info(f"Created table {full_table_name}")

            # Insert data
            insert_sql = f"INSERT INTO {full_table_name} SELECT * FROM temp_{table_name}"
            spark.sql(insert_sql)

        logger.info(f"Successfully created S3 Table: {full_table_name}")

        # Verify the write
        verification_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
        logger.info(f"Verification: {verification_count} records in {full_table_name}")

        # Show sample data
        logger.info(f"Sample data from {full_table_name}:")
        spark.sql(f"SELECT * FROM {full_table_name} LIMIT 5").show()

        return True

    except Exception as e:
        logger.error(f"Error writing table {table_name} to S3 Tables: {str(e)}")
        return False

def verify_s3_tables_connection():
    """Verify S3 Tables REST endpoint connection"""
    try:
        logger.info("Verifying S3 Tables REST endpoint connection...")

        # Show catalogs
        logger.info("Available catalogs:")
        spark.sql("SHOW CATALOGS").show()

        # Show namespaces
        logger.info("Available namespaces in s3_rest_catalog:")
        spark.sql("SHOW NAMESPACES IN s3_rest_catalog").show()

        return True
    except Exception as e:
        logger.error(f"S3 Tables REST endpoint connection verification failed: {str(e)}")
        return False

# Main ETL process
def main():
    """Main ETL process to migrate tables from Aurora MySQL to S3 Tables"""
    logger.info("Starting Aurora MySQL to S3 Tables migration using REST endpoint")
    logger.info(f"Source: Aurora MySQL database '{database}'")
    logger.info(f"Target: S3 Tables namespace '{s3_table_namespace}'")

    # Verify S3 Tables connection
    if not verify_s3_tables_connection():
        logger.error("Failed to verify S3 Tables REST endpoint connection. Exiting.")
        return

    successful_migrations = 0
    failed_migrations = 0
    migration_summary = []

    for table_name, config in tables_config.items():
        try:
            logger.info(f"Processing table: {table_name} - {config['description']}")

            # Read from Aurora MySQL
            mysql_data = read_mysql_table(table_name)

            if mysql_data is not None:
                # Write to S3 Tables in Iceberg format
                success = write_to_s3_tables_iceberg(mysql_data, table_name)

                if success:
                    successful_migrations += 1
                    status = "SUCCESS"
                    logger.info(f"✓ Successfully migrated {table_name}")
                else:
                    failed_migrations += 1
                    status = "FAILED"
                    logger.error(f"✗ Failed to migrate {table_name}")
            else:
                failed_migrations += 1
                status = "FAILED - READ ERROR"
                logger.error(f"✗ Failed to read {table_name} from MySQL")

            migration_summary.append({
                "table": table_name,
                "status": status,
                "primary_key": config["primary_key"]
            })

        except Exception as e:
            failed_migrations += 1
            logger.error(f"✗ Unexpected error processing {table_name}: {str(e)}")
            migration_summary.append({
                "table": table_name,
                "status": f"ERROR: {str(e)}",
                "primary_key": config.get("primary_key", "unknown")
            })

    # Print migration summary
    logger.info("=" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 60)
    for summary in migration_summary:
        logger.info(f"Table: {summary['table']:<15} | Status: {summary['status']:<20} | PK: {summary['primary_key']}")

    logger.info("=" * 60)
    logger.info(f"Migration completed: {successful_migrations} successful, {failed_migrations} failed")

    if failed_migrations > 0:
        logger.warning(f"Migration completed with {failed_migrations} failures")
    else:
        logger.info("All tables migrated successfully to S3 Tables!")

# Execute main process
if __name__ == "__main__":
    main()
    job.commit()
