import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuration
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-catalog:8181")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
WAREHOUSE_PATH = "s3a://warehouse/"

def get_spark_session():
    # We include packages required for Iceberg REST Catalog and AWS S3 FileIO
    return SparkSession.builder \
        .appName("BronzeToSilver") \
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

# Explicit schemas for raw json data
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("updated_at", StringType(), True)
])

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("inventory_count", IntegerType(), True),
    StructField("updated_at", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True)
])

order_item_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("updated_at", StringType(), True)
])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Incremental processing date (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    
    dt = datetime.strptime(args.date, "%Y-%m-%d")
    year, month, day = args.date.split("-")
    
    spark = get_spark_session()
    
    # 1. Initialize Iceberg database if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.db")
    
    # 2. Process CUSTOMERS
    print("Processing Customers...")
    raw_customers_path = f"s3a://raw/customers/customers.json"
    df_cust = spark.read.schema(customer_schema).json(raw_customers_path)
    df_cust_clean = df_cust \
        .withColumn("signup_date", to_timestamp(col("signup_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.customers (
            customer_id STRING,
            name STRING,
            email STRING,
            country STRING,
            signup_date TIMESTAMP,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    df_cust_clean.createOrReplaceTempView("incoming_customers")
    spark.sql("""
        MERGE INTO demo.db.customers t
        USING incoming_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET 
            t.name = s.name,
            t.email = s.email,
            t.country = s.country,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Customers merge completed.")

    # 3. Process PRODUCTS
    print("Processing Products...")
    raw_products_path = f"s3a://raw/products/products.json"
    df_prod = spark.read.schema(product_schema).json(raw_products_path)
    df_prod_clean = df_prod \
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.products (
            product_id STRING,
            name STRING,
            category STRING,
            price DOUBLE,
            inventory_count INT,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    df_prod_clean.createOrReplaceTempView("incoming_products")
    spark.sql("""
        MERGE INTO demo.db.products t
        USING incoming_products s
        ON t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET 
            t.name = s.name,
            t.category = s.category,
            t.price = s.price,
            t.inventory_count = s.inventory_count,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Products merge completed.")

    # 4. Process ORDERS (Incremental partition)
    print(f"Processing Orders for {args.date}...")
    raw_orders_path = f"s3a://raw/orders/year={year}/month={month}/day={day}/*.json"
    df_orders = spark.read.schema(order_schema).json(raw_orders_path)
    df_orders_clean = df_orders \
        .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.orders (
            order_id STRING,
            customer_id STRING,
            order_date TIMESTAMP,
            total_amount DOUBLE,
            status STRING,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(order_date))
    """)
    df_orders_clean.createOrReplaceTempView("incoming_orders")
    spark.sql("""
        MERGE INTO demo.db.orders t
        USING incoming_orders s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET 
            t.total_amount = s.total_amount,
            t.status = s.status,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Orders merge completed.")

    # 5. Process ORDER ITEMS (Incremental partition)
    print(f"Processing Order Items for {args.date}...")
    raw_items_path = f"s3a://raw/order_items/year={year}/month={month}/day={day}/*.json"
    df_items = spark.read.schema(order_item_schema).json(raw_items_path)
    df_items_clean = df_items \
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
        
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.db.order_items (
            order_item_id STRING,
            order_id STRING,
            product_id STRING,
            quantity INT,
            unit_price DOUBLE,
            updated_at TIMESTAMP
        ) USING iceberg
    """)
    df_items_clean.createOrReplaceTempView("incoming_order_items")
    spark.sql("""
        MERGE INTO demo.db.order_items t
        USING incoming_order_items s
        ON t.order_item_id = s.order_item_id
        WHEN MATCHED THEN UPDATE SET 
            t.quantity = s.quantity,
            t.unit_price = s.unit_price,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Order Items merge completed.")
    
    print("Bronze to Silver transformation completed successfully!")

if __name__ == "__main__":
    main()
