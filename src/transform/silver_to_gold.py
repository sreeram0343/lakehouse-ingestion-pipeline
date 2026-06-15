import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, to_date, lit, min, max

# Configuration
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-catalog:8181")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
WAREHOUSE_PATH = "s3a://warehouse/"

def get_spark_session():
    return SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", ICEBERG_CATALOG_URI) \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", WAREHOUSE_PATH) \
        .config("spark.sql.catalog.demo.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
        .config("spark.sql.catalog.demo.s3.access-key-id", MINIO_ACCESS_KEY) \
        .config("spark.sql.catalog.demo.s3.secret-access-key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    run_timestamp = datetime.now()
    run_timestamp_str = run_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    print("Reading Silver tables...")
    orders = spark.read.table("demo.db.orders")
    order_items = spark.read.table("demo.db.order_items")
    customers = spark.read.table("demo.db.customers")
    products = spark.read.table("demo.db.products")
    
    # ----------------------------------------------------
    # 1. GOLD TABLE: Daily Sales Performance
    # ----------------------------------------------------
    print("Aggregating Daily Sales...")
    daily_sales = orders \
        .withColumn("order_date_only", to_date(col("order_date"))) \
        .groupBy("order_date_only") \
        .agg(
            sum(col("total_amount")).alias("total_revenue"),
            count(col("order_id")).alias("orders_count"),
            avg(col("total_amount")).alias("average_order_value"),
            sum(col("status") == "Cancelled").alias("cancelled_orders_count") # True is 1, false 0 in aggregate
        ) \
        .withColumn("updated_at", lit(run_timestamp))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.gold_daily_sales (
            order_date DATE,
            total_revenue DOUBLE,
            orders_count LONG,
            average_order_value DOUBLE,
            cancelled_orders_count LONG,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    daily_sales.createOrReplaceTempView("incoming_daily_sales")
    spark.sql("""
        MERGE INTO demo.db.gold_daily_sales t
        USING incoming_daily_sales s
        ON t.order_date = s.order_date_only
        WHEN MATCHED THEN UPDATE SET 
            t.total_revenue = s.total_revenue,
            t.orders_count = s.orders_count,
            t.average_order_value = s.average_order_value,
            t.cancelled_orders_count = s.cancelled_orders_count,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Daily Sales Gold merge completed.")

    # ----------------------------------------------------
    # 2. GOLD TABLE: Customer Purchase Metrics
    # ----------------------------------------------------
    print("Aggregating Customer Metrics...")
    customer_metrics = orders \
        .groupBy("customer_id") \
        .agg(
            count(col("order_id")).alias("total_orders"),
            sum(col("total_amount")).alias("total_spent"),
            min(col("order_date")).alias("first_order_date"),
            max(col("order_date")).alias("last_order_date")
        ) \
        .join(customers, "customer_id") \
        .select(
            col("customer_id"),
            col("name").alias("customer_name"),
            col("email"),
            col("country"),
            col("total_orders"),
            col("total_spent"),
            col("first_order_date"),
            col("last_order_date")
        ) \
        .withColumn("updated_at", lit(run_timestamp))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.gold_customer_metrics (
            customer_id STRING,
            customer_name STRING,
            email STRING,
            country STRING,
            total_orders LONG,
            total_spent DOUBLE,
            first_order_date TIMESTAMP,
            last_order_date TIMESTAMP,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    customer_metrics.createOrReplaceTempView("incoming_customer_metrics")
    spark.sql("""
        MERGE INTO demo.db.gold_customer_metrics t
        USING incoming_customer_metrics s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET 
            t.customer_name = s.customer_name,
            t.email = s.email,
            t.country = s.country,
            t.total_orders = s.total_orders,
            t.total_spent = s.total_spent,
            t.first_order_date = s.first_order_date,
            t.last_order_date = s.last_order_date,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Customer Metrics Gold merge completed.")

    # ----------------------------------------------------
    # 3. GOLD TABLE: Category Performance
    # ----------------------------------------------------
    print("Aggregating Category Performance...")
    category_performance = order_items \
        .join(products, "product_id") \
        .groupBy("category") \
        .agg(
            sum(col("quantity")).alias("units_sold"),
            sum(col("quantity") * col("unit_price")).alias("revenue_generated")
        ) \
        .withColumn("updated_at", lit(run_timestamp))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.gold_category_performance (
            category STRING,
            units_sold LONG,
            revenue_generated DOUBLE,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    category_performance.createOrReplaceTempView("incoming_category_perf")
    spark.sql("""
        MERGE INTO demo.db.gold_category_performance t
        USING incoming_category_perf s
        ON t.category = s.category
        WHEN MATCHED THEN UPDATE SET 
            t.units_sold = s.units_sold,
            t.revenue_generated = s.revenue_generated,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Category Performance Gold merge completed.")
    
    print("Silver to Gold transformation completed successfully!")

if __name__ == "__main__":
    main()
