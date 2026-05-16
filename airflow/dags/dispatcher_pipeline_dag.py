# airflow/dags/dispatcher_pipeline_dag.py

import os
import sys
from datetime import datetime, timedelta

# Make python-etl package visible
sys.path.append("/opt/python-etl")

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

from etl_manager import run_pipeline_for_object

# ---- Config (kept env-driven so docker-compose can control it) ----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

LANDING_BUCKET = os.getenv("LANDING_BUCKET", "lakehouse-data")  # where raw lands
ARCHIVE_BUCKET = os.getenv("ARCHIVE_BUCKET", "raw-data")        # long-term raw archive


def _get_pg_conn():
    """Fresh psycopg2 connection for catalog updates inside the DAG."""
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "lakehouse_db"),
        user=os.getenv("POSTGRES_USER", "lakehouse_user"),
        password=os.getenv("POSTGRES_PASSWORD", "lakehouse_pass"),
    )


def _catalog_upsert(pg_conn, bucket_name, object_name, object_size=None,
                    file_format=None, row_count=None, text_extracted=False,
                    uploaded_by=None, metadata=None):
    """
    Upsert a row into minio_data_catalog.
    Used here specifically to track the raw-data archive copy.
    content_hash has been removed as it is deprecated.
    """
    import json
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO minio_data_catalog
                (bucket_name, object_name, object_size, file_format,
                 row_count, text_extracted, uploaded_by, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (bucket_name, object_name) DO UPDATE
            SET object_size    = EXCLUDED.object_size,
                file_format    = EXCLUDED.file_format,
                row_count      = EXCLUDED.row_count,
                text_extracted = EXCLUDED.text_extracted,
                uploaded_by    = EXCLUDED.uploaded_by,
                metadata       = EXCLUDED.metadata,
                last_modified  = CURRENT_TIMESTAMP
            """,
            (
                bucket_name, object_name, object_size, file_format,
                row_count, text_extracted, uploaded_by,
                json.dumps(metadata or {}),
            ),
        )
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"[DISPATCHER] catalog_upsert failed for {bucket_name}/{object_name}: {e}")
    finally:
        cursor.close()


def _catalog_mark_archived(pg_conn, bucket_name, object_name, archive_bucket, archive_key):
    """
    Update the raw/ catalog entry's metadata to record that the file
    was archived, instead of deleting the catalog row outright.
    This preserves the audit trail — you can see what was uploaded even
    after the landing copy is gone.
    """
    import json
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE minio_data_catalog
            SET metadata = metadata || %s::jsonb,
                last_modified = CURRENT_TIMESTAMP
            WHERE bucket_name = %s AND object_name = %s
            """,
            (
                json.dumps({
                    "archived":        True,
                    "archive_bucket":  archive_bucket,
                    "archive_key":     archive_key,
                    "archived_at":     datetime.utcnow().isoformat(),
                }),
                bucket_name,
                object_name,
            ),
        )
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"[DISPATCHER] mark_archived failed for {bucket_name}/{object_name}: {e}")
    finally:
        cursor.close()


def scan_and_dispatch():
    """
    Scan landing bucket raw/, run ETL pipelines and
    archive original raw files by date into ARCHIVE_BUCKET.

    Catalog tracking:
      - ETL pipeline (run_pipeline_for_object) writes processed entries to
        minio_data_catalog under bucket 'lakehouse-data' as before.
      - After archiving, we additionally register the raw-data copy in
        minio_data_catalog (bucket='raw-data') so it's visible in the catalog.
      - The original landing raw/ entry is updated with archive metadata
        (not deleted) to preserve the audit trail.
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # Ensure archive bucket exists
    if not client.bucket_exists(ARCHIVE_BUCKET):
        client.make_bucket(ARCHIVE_BUCKET)

    pg_conn = _get_pg_conn()
    pg_conn.autocommit = False

    try:
        # List all objects under raw/ in landing bucket
        objects = list(client.list_objects(LANDING_BUCKET, prefix="raw/", recursive=True))

        for obj in objects:
            object_name = obj.object_name

            # Skip "directory" markers
            if object_name.endswith("/"):
                continue

            print(f"[DISPATCHER] Found file: {object_name}")

            # 1. Run ETL pipeline — writes processed catalog entries under
            #    bucket='lakehouse-data'.  Raises on error → caught below.
            try:
                run_pipeline_for_object(object_name)
            except Exception as pipeline_err:
                print(f"[DISPATCHER] Pipeline failed for {object_name}: {pipeline_err}")
                # Don't archive if pipeline failed — leave in landing for retry
                raise

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

            # 3. Register the archive copy in minio_data_catalog
            #    (bucket = 'raw-data', object_name = YYYY-MM-DD/filename)
            ext = base_name.rsplit(".", 1)[-1].lower() if "." in base_name else ""
            format_map = {
                "csv": "structured", "json": "structured", "parquet": "structured",
                "pdf": "pdf", "docx": "docx", "doc": "docx",
                "png": "image", "jpg": "image", "jpeg": "image", "tiff": "image",
                "ppt": "ppt", "pptx": "ppt",
            }
            file_format = format_map.get(ext, ext)

            # Read uploaded_by from object metadata to carry it forward
            try:
                stat = client.stat_object(LANDING_BUCKET, object_name)
                uploaded_by_raw = stat.metadata.get("x-amz-meta-uploaded-by")
                uploaded_by = int(uploaded_by_raw) if uploaded_by_raw else None
                object_size = obj.size
            except Exception:
                uploaded_by = None
                object_size = None

            _catalog_upsert(
                pg_conn,
                bucket_name=ARCHIVE_BUCKET,
                object_name=archive_key,
                object_size=object_size,
                file_format=file_format,
                uploaded_by=uploaded_by,
                metadata={
                    "source":              "dispatcher_archive",
                    "original_object":     object_name,
                    "original_bucket":     LANDING_BUCKET,
                    "archived_at":         datetime.utcnow().isoformat(),
                    "original_filename":   base_name,
                },
            )
            print(f"[DISPATCHER] Catalog updated for archive: {ARCHIVE_BUCKET}/{archive_key}")

            # 4. Mark the original landing entry as archived (keep the row)
            _catalog_mark_archived(
                pg_conn,
                bucket_name=LANDING_BUCKET,
                object_name=object_name,
                archive_bucket=ARCHIVE_BUCKET,
                archive_key=archive_key,
            )

            # 5. Remove original from landing raw/ so it is never reprocessed
            client.remove_object(LANDING_BUCKET, object_name)
            print(f"[DISPATCHER] Completed & removed landing copy: {object_name}\n")

    finally:
        pg_conn.close()


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