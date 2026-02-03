# airflow/dags/dispatcher_pipeline_dag.py

import os
import sys
from datetime import datetime, timedelta

# Make python-etl package visible
sys.path.append("/opt/python-etl")

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

# from etl_manager import run_pipeline_for_object  <-- Moved inside function

# ---- Config (kept env-driven so docker-compose can control it) ----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

LANDING_BUCKET = os.getenv("LANDING_BUCKET", "lakehouse-data")  # where raw lands
ARCHIVE_BUCKET = os.getenv("ARCHIVE_BUCKET", "raw-data")        # long-term raw archive


def scan_and_dispatch():
    """
    Scan landing bucket raw/, run ETL pipelines and
    archive original raw files by date into ARCHIVE_BUCKET.
    """
    from etl_manager import run_pipeline_for_object
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # Ensure archive bucket exists
    if not client.bucket_exists(ARCHIVE_BUCKET):
        client.make_bucket(ARCHIVE_BUCKET)

    # List all objects under raw/ in landing bucket
    objects = client.list_objects(LANDING_BUCKET, prefix="raw/", recursive=True)

    for obj in objects:
        object_name = obj.object_name

        # Skip "directory" markers
        if object_name.endswith("/"):
            continue

        print(f"[DISPATCHER] Found file: {object_name}")

        # 1. Run your universal ETL pipeline
        #    (inside it, structured/pdf/docx/image pipelines are called
        #     and update_catalog is used.)
        run_pipeline_for_object(object_name)

        # 2. Build archive key: raw-data/YYYY-MM-DD/filename
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        base_name = object_name.split("/")[-1]
        archive_key = f"{today_str}/{base_name}"

        print(f"[DISPATCHER] Archiving {object_name} -> {ARCHIVE_BUCKET}/{archive_key}")

        from minio.commonconfig import CopySource
        # Copy original raw into archive bucket
        client.copy_object(
            ARCHIVE_BUCKET,
            archive_key,
            CopySource(LANDING_BUCKET, object_name),
        )

        # 3. Remove original from landing raw/ so it is never reprocessed
        client.remove_object(LANDING_BUCKET, object_name)

        print(f"[DISPATCHER] Completed & removed landing copy: {object_name}\n")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="distillationDAG",
    description="Scans landing raw/ and runs ETL pipelines, archiving originals by date",
    default_args=default_args,
    schedule=timedelta(seconds=45),  # every 45 seconds
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    dispatch_task = PythonOperator(
        task_id="scan_minio_and_dispatch",
        python_callable=scan_and_dispatch,
    )
