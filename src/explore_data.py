import os
from pyspark.sql import SparkSession

# Configuration
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-catalog:8181")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
WAREHOUSE_PATH = "s3a://warehouse/"

def get_spark_session():
    return SparkSession.builder \
        .appName("LakehouseExplorer") \
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
    
    print("\n" + "="*50)
    print(" LAKEHOUSE PIPELINE EXPLORER & VALIDATION")
    print("="*50)
    
    # List catalogs / schemas
    print("\n--- Available Catalogs / Namespaces ---")
    try:
        namespaces = spark.sql("SHOW NAMESPACES IN demo").collect()
        for ns in namespaces:
            print(f"Namespace: {ns[0]}")
    except Exception as e:
        print(f"Error listing namespaces: {e}. (Have any tables been created yet?)")
        return

    # List tables
    print("\n--- Available Tables in demo.db ---")
    try:
        tables = spark.sql("SHOW TABLES IN demo.db").collect()
        table_names = []
        for t in tables:
            full_name = f"demo.db.{t[1]}"
            table_names.append(full_name)
            print(f"Table: {full_name}")
    except Exception as e:
        print(f"Error listing tables: {e}")
        return

    # Query tables
    for tbl in table_names:
        print("\n" + "-"*40)
        print(f" Table: {tbl} ")
        print("-"*40)
        df = spark.read.table(tbl)
        print(f"Total rows: {df.count()}")
        print("Schema:")
        df.printSchema()
        print("Sample data:")
        df.show(5, truncate=False)
        
    # Execute a custom multi-table analytical query to prove reporting utility
    print("\n" + "="*50)
    print(" ANALYTICAL QUERY DEMONSTRATION")
    print("="*50)
    try:
        query = """
            SELECT 
                c.country,
                p.category,
                SUM(i.quantity) as total_units_sold,
                ROUND(SUM(i.quantity * i.unit_price), 2) as total_revenue
            FROM demo.db.order_items i
            JOIN demo.db.orders o ON i.order_id = o.order_id
            JOIN demo.db.customers c ON o.customer_id = c.customer_id
            JOIN demo.db.products p ON i.product_id = p.product_id
            WHERE o.status != 'Cancelled'
            GROUP BY c.country, p.category
            ORDER BY total_revenue DESC, total_units_sold DESC
        """
        print("Executing cross-table join (Orders + Items + Customers + Products)...")
        res = spark.sql(query)
        res.show(20, truncate=False)
    except Exception as e:
        print(f"Could not run analytical query: {e}")

if __name__ == "__main__":
    main()
