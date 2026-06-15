import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    "owner": "lakehouse_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Environment variables for our pipeline scripts
PIPELINE_ENV = {
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "admin"),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "supersecretpassword"),
    "ICEBERG_CATALOG_URI": os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-catalog:8181"),
}

with DAG(
    "lakehouse_ingestion_pipeline",
    default_args=default_args,
    description="End-to-End Lakehouse Ingestion Pipeline (Bronze -> Silver -> Gold) using Spark, Iceberg, and MinIO",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 10),
    catchup=True,
    max_active_runs=1,
    tags=["lakehouse", "spark", "iceberg"],
) as dag:

    # 1. Ingestion: Generate synthetic e-commerce transactional and dimensional data and write to landing zone (MinIO raw bucket)
    generate_mock_data = BashOperator(
        task_id="generate_mock_data",
        bash_command="python /opt/airflow/src/ingest/generate_mock_data.py --date {{ ds }}",
        env=PIPELINE_ENV,
    )

    # 2. Bronze to Silver: Clean raw data and upsert into Iceberg tables (demo.db.customers, demo.db.products, etc.)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /opt/airflow/src/transform/bronze_to_silver.py --date {{ ds }}",
        env=PIPELINE_ENV,
    )

    # 3. Silver to Gold: Aggregate metrics and upsert into analytical reporting tables (demo.db.gold_daily_sales, etc.)
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /opt/airflow/src/transform/silver_to_gold.py",
        env=PIPELINE_ENV,
    )

    # Task dependencies
    generate_mock_data >> bronze_to_silver >> silver_to_gold
