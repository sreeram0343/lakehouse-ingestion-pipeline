"""
Distributed Transformation Layer (Phase 4).
Reads raw JSON data from landing-zone bucket (Bronze), flattens the nested structures,
enforces schema constraints, deduplicates, and commits the clean data to the warehouse
bucket (Silver) using the Apache Iceberg format.
"""

import os
import sys

# Ensure the project root is in the Python path to resolve config imports correctly.
# This allows running the script from any directory context.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config.logger_config import get_logger
from config.ingest_config import IngestConfig

# Initialize Logger
logger = get_logger("spark_iceberg_transform")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ImportError as imp_err:
    logger.error(f"🔴 PySpark is not installed or not available in the current environment: {imp_err}")
    # We will log the error but let the create_spark_session handle raising it or exiting.


def create_spark_session() -> SparkSession:
    """
    Initializes a SparkSession explicitly configured to integrate with MinIO/S3
    and the standalone Iceberg REST Catalog.
    """
    logger.info("Initializing SparkSession with Apache Iceberg and MinIO configs...")
    try:
        # Load configuration variables from IngestConfig
        IngestConfig.validate()
        s3_endpoint = IngestConfig.S3_ENDPOINT_URL
        access_key = IngestConfig.AWS_ACCESS_KEY_ID
        secret_key = IngestConfig.AWS_SECRET_ACCESS_KEY

        # Determine catalog URI (localhost for host runs, iceberg-catalog for container runs)
        catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")

        # Define dependencies to fetch if not pre-cached (e.g., when running outside docker)
        packages = [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "software.amazon.awssdk:bundle:2.20.18",
            "software.amazon.awssdk:url-connection-client:2.20.18"
        ]

        spark = SparkSession.builder \
            .appName("SparkIcebergTransform") \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.lakehouse.type", "rest") \
            .config("spark.sql.catalog.lakehouse.uri", catalog_uri) \
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/") \
            .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint) \
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
            .config("spark.sql.catalog.lakehouse.s3.access-key-id", access_key) \
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key", secret_key) \
            .config("spark.sql.catalog.lakehouse.s3.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

        logger.info("🟢 SparkSession successfully created.")
        return spark
    except Exception as e:
        logger.error(f"🔴 Failed to initialize SparkSession. Error: {e}")
        raise


def extract_bronze_data(spark: SparkSession):
    """
    Reads the raw Bronze JSON data from the landing-zone S3 bucket.
    """
    # MinIO raw ingestion path (landing-zone bucket)
    bronze_path = f"s3a://{IngestConfig.TARGET_BUCKET_NAME}/crypto_data/"
    logger.info(f"Extracting raw Bronze data from location: {bronze_path}")
    try:
        df = spark.read.json(bronze_path)
        logger.info("🟢 Successfully loaded raw Bronze JSON data.")
        return df
    except Exception as e:
        logger.error(f"🔴 Failed to read Bronze data from {bronze_path}. Error: {e}")
        raise


def transform_and_deduplicate(df):
    """
    Flattens the nested coin price JSON structure, casts fields to their correct types,
    enforces a strict schema, and deduplicates based on coin_id and timestamp.
    """
    logger.info("Transforming and deduplicating Bronze data...")
    try:
        # 1. Identify coin columns dynamically (ignoring corrupt record markers)
        coin_cols = [col_name for col_name in df.columns if col_name not in ("_corrupt_record",)]
        logger.info(f"Identified coin columns in schema: {coin_cols}")

        if not coin_cols:
            raise ValueError("No valid coin columns found in the input DataFrame schema.")

        # 2. Build map expression to transform columns into a map of struct fields
        # This dynamically flattens columns like `bitcoin`, `ethereum` into key-value pairs.
        map_expr = []
        for coin in coin_cols:
            map_expr.append(F.lit(coin))
            map_expr.append(F.struct(
                F.col(f"{coin}.usd").alias("price_usd"),
                F.col(f"{coin}.usd_market_cap").alias("market_cap_usd"),
                F.col(f"{coin}.usd_24h_vol").alias("volume_24h_usd"),
                F.col(f"{coin}.usd_24h_change").alias("change_24h_percent"),
                F.col(f"{coin}.last_updated_at").alias("last_updated_at")
            ))

        # Add map column and explode it to create dynamic rows
        df_map = df.withColumn("coin_map", F.create_map(*map_expr))
        df_exploded = df_map.select(F.explode("coin_map").alias("coin_id", "metrics"))

        # 3. Select final columns, casting appropriately
        df_flattened = df_exploded.select(
            F.col("coin_id").cast("string").alias("coin_id"),
            F.col("metrics.price_usd").cast("double").alias("price_usd"),
            F.col("metrics.market_cap_usd").cast("double").alias("market_cap_usd"),
            F.col("metrics.volume_24h_usd").cast("double").alias("volume_24h_usd"),
            F.col("metrics.change_24h_percent").cast("double").alias("change_24h_percent"),
            # Cast unix epoch seconds to timestamp
            F.from_unixtime(F.col("metrics.last_updated_at")).cast("timestamp").alias("timestamp")
        )

        # 4. Deduplicate based on 'coin_id' and 'timestamp' using Window function row_number()
        window_spec = Window.partitionBy("coin_id", "timestamp").orderBy(F.lit(1))
        df_dedup = df_flattened.withColumn("rn", F.row_number().over(window_spec)) \
                               .filter(F.col("rn") == 1) \
                               .drop("rn")

        logger.info("🟢 Transformation and deduplication complete.")
        return df_dedup

    except Exception as e:
        logger.error(f"🔴 Transformation failed: {e}")
        raise


def load_to_silver_iceberg(df):
    """
    Writes the conformed and deduplicated DataFrame to the Apache Iceberg Silver table.
    Ensures that the catalog database namespace exists and handles table creation dynamically.
    """
    table_name = "lakehouse.silver.crypto_prices"
    logger.info(f"Loading transformed data to Iceberg table: {table_name}")
    try:
        # Get active spark session from dataframe
        spark = df.sparkSession

        # Ensure namespace exists
        logger.info("Ensuring database namespace 'lakehouse.silver' exists...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

        # Check if table exists
        if not spark.catalog.tableExists(table_name):
            logger.info(f"Table {table_name} does not exist. Creating and saving first snapshot...")
            df.writeTo(table_name) \
              .tableProperty("write.format.default", "parquet") \
              .create()
            logger.info(f"🟢 Table {table_name} successfully created.")
        else:
            logger.info(f"Table {table_name} already exists. Appending batch data...")
            df.writeTo(table_name).append()
            logger.info(f"🟢 Batch data successfully appended to {table_name}.")

    except Exception as e:
        logger.error(f"🔴 Failed to write data to Iceberg. Error: {e}")
        raise


def main():
    logger.info("=" * 60)
    logger.info("STARTING PHASE 4 TASK: DISTRIBUTED TRANSFORMATION (SPARK -> ICEBERG)")
    logger.info("=" * 60)

    spark = None
    try:
        # 1. Initialize SparkSession
        spark = create_spark_session()

        # 2. Extract Bronze Data
        df_bronze = extract_bronze_data(spark)

        # 3. Transform and Deduplicate Data
        df_silver = transform_and_deduplicate(df_bronze)

        # 4. Load Data to Iceberg (Silver)
        load_to_silver_iceberg(df_silver)

        logger.info("=" * 60)
        logger.info("🟢 SUCCESS: Distributed Transformation execution completed successfully.")
        logger.info("=" * 60)
        sys.exit(0)

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"🔴 FAILURE: Distributed transformation job failed. Details: {e}")
        logger.error("=" * 60)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping active SparkSession...")
            spark.stop()
            logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
