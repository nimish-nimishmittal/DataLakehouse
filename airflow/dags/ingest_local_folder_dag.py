# airflow/dags/ingest_local_folder_dag.py

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

import psycopg2
from datetime import datetime
import magic

# Landing bucket (same as Flask uploader)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("LANDING_BUCKET", "lakehouse-data")

LOCAL_FOLDER = "/opt/airflow/sample"


def ingest_local_files():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # Ensure landing bucket exists
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    for fname in os.listdir(LOCAL_FOLDER):
        fpath = os.path.join(LOCAL_FOLDER, fname)

        if not os.path.isfile(fpath):
            continue

        object_name = f"raw/{fname}"

        with open(fpath, "rb") as f:
            size = os.path.getsize(fpath)
            client.put_object(
                MINIO_BUCKET,
                object_name,
                f,
                length=size,
                content_type="application/octet-stream",
            )

        print(f"[INGEST] Uploaded {fname} -> {MINIO_BUCKET}/raw/")

        # NEW: Basic metadata for bulk
        mime_type = magic.from_file(fpath, mime=True)
        basic_metadata = {
            'original_filename': fname,
            'mime_type': mime_type,
            'ingest_time': datetime.utcnow().isoformat(),
            'source': 'local_folder'
        }

        # Connect to DB (add your creds)
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'lakehouse_db'),
            user=os.getenv('POSTGRES_USER', 'lakehouse_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'lakehouse_pass')
        )
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO minio_data_catalog (bucket_name, object_name, object_size, file_format, uploaded_by, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (bucket_name, object_name) DO UPDATE
                SET object_size = EXCLUDED.object_size,
                    metadata = EXCLUDED.metadata,
                    last_modified = CURRENT_TIMESTAMP
            """, (MINIO_BUCKET, object_name, size, fname.rsplit('.', 1)[-1].lower() if '.' in fname else None, None, json.dumps(basic_metadata)))  # uploaded_by=None for bulk
            conn.commit()
        except Exception as e:
            print(f"[INGEST] Catalog failed: {e}")
        finally:
            cursor.close()
            conn.close()

        os.remove(fpath)

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="ingestionDAG",  # <= your clean name for the ingestion DAG
    default_args=default_args,
    description="Loads files from local folder into MinIO landing raw/",
    schedule=timedelta(seconds=30),  # every 30 seconds
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_local_folder",
        python_callable=ingest_local_files,
    )
