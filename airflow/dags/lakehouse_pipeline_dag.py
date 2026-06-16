"""
Resilient Orchestration DAG (Phase 5).
Configures daily schedules to trigger raw API data extraction (Bronze), DVC lineage
tracking, and dynamic PySpark transformation (Silver Iceberg Sink) sequentially.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Define default arguments including DAG level retry configurations
default_args = {
    "owner": "lakehouse_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# 2. Define daily scheduled DAG
with DAG(
    "lakehouse_pipeline_dag",
    default_args=default_args,
    description="End-to-End Lakehouse Ingestion & Lineage Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 10),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dvc", "spark", "iceberg"],
) as dag:

    # Task 1: Extract API data to landing zone (MinIO raw bucket)
    # Includes robust task-specific retry policy (3 retries, 5-minute backoff)
    extract_api = BashOperator(
        task_id="Extract_API",
        bash_command="python /opt/airflow/src/ingest/api_to_s3.py",
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # Task 2: Snapshot and DVC-track the raw data prefix
    snapshot_dvc = BashOperator(
        task_id="Snapshot_DVC",
        bash_command="python /opt/airflow/src/ingest/dvc_tracker.py",
    )

    # Task 3: Distributed Transformation via PySpark & Apache Iceberg
    transform_spark = BashOperator(
        task_id="Transform_Spark",
        bash_command="python /opt/airflow/src/transform/spark_iceberg_transform.py",
    )

    # 3. Enforce execution order: Extract -> Snapshot -> Transform
    extract_api >> snapshot_dvc >> transform_spark
