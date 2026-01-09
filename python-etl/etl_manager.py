import os
import io
import pandas as pd
from minio import Minio
from minio.error import S3Error
import psycopg2
from psycopg2.extras import execute_values
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LakehouseETL:
    def __init__(self):
        # Initialize MinIO client
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
            secure=False
        )
        
        # Initialize PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'lakehouse_db'),
            user=os.getenv('POSTGRES_USER', 'lakehouse_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'lakehouse_pass')
        )
        self.pg_conn.autocommit = False
        self.bucket_name = 'lakehouse-data'
    
    def update_catalog(
        self,
        object_name: str,
        object_size: int | None = None,
        file_format: str | None = None,
        row_count: int | None = None,
        text_extracted: bool = False,
        content_hash: str | None = None,
        uploaded_by: int | None = None,
        metadata: dict | None = None,  # NEW
    ):
        """
        Upsert entry into minio_data_catalog table.
        """
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS minio_data_catalog (
                    catalog_id SERIAL PRIMARY KEY,
                    bucket_name TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    object_size BIGINT,
                    file_format TEXT,
                    row_count INTEGER,
                    text_extracted BOOLEAN DEFAULT FALSE,
                    content_hash TEXT,
                    last_modified TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    uploaded_by INTEGER,
                    UNIQUE(bucket_name, object_name)
                )
                """
            )

            cursor.execute(
                """
                ALTER TABLE minio_data_catalog ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::JSONB
                """
            )

            cursor.execute(
                """
                INSERT INTO minio_data_catalog
                (bucket_name, object_name, object_size, file_format, row_count, text_extracted, content_hash, uploaded_by, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bucket_name, object_name) DO UPDATE
                SET object_size = EXCLUDED.object_size,
                    file_format = EXCLUDED.file_format,
                    row_count = EXCLUDED.row_count,
                    text_extracted = EXCLUDED.text_extracted,
                    content_hash = EXCLUDED.content_hash,
                    uploaded_by = EXCLUDED.uploaded_by,
                    metadata = EXCLUDED.metadata,  -- NEW
                    last_modified = CURRENT_TIMESTAMP
                """,
                (self.bucket_name, object_name, object_size, file_format, row_count, text_extracted, content_hash, uploaded_by, json.dumps(metadata or {}))
            )
            self.pg_conn.commit()
        finally:
            cursor.close()

    
    def ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
    
    def read_csv_from_minio(self, object_name):
        """Read CSV from MinIO into pandas DataFrame"""
        response = None
        try:
            response = self.minio_client.get_object(self.bucket_name, object_name)
            df = pd.read_csv(io.BytesIO(response.read()))
            logger.info(f"Read {len(df)} rows from {object_name}")
            return df
        except S3Error as e:
            logger.error(f"Error reading file: {e}")
            return None
        finally:
            if response:
                response.close()
                response.release_conn()
    
    def write_parquet_to_minio(self, df, object_name):
        """Write DataFrame to Parquet in MinIO"""
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            logger.info(f"Wrote Parquet: {object_name}")
        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
    
    def load_dataframe_to_postgres(self, df, table_name):
        """Load DataFrame into PostgreSQL table"""
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table schema
            columns = []
            for col, dtype in df.dtypes.items():
                if dtype == 'int64':
                    pg_type = 'INTEGER'
                elif dtype == 'float64':
                    pg_type = 'NUMERIC(12,2)'
                else:
                    pg_type = 'TEXT'
                columns.append(f"{col} {pg_type}")
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            # Bulk insert
            cols = ','.join(df.columns)
            values = [tuple(x) for x in df.to_numpy()]
            execute_values(
                cursor, 
                f"INSERT INTO {table_name} ({cols}) VALUES %s", 
                values
            )
            
            self.pg_conn.commit()
            logger.info(f"Loaded {len(df)} rows into {table_name}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Error loading to PostgreSQL: {e}")
        finally:
            cursor.close()
    
    def create_data_catalog(self):
        """Create metadata catalog for MinIO objects"""
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minio_data_catalog (
                    catalog_id SERIAL PRIMARY KEY,
                    bucket_name TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    object_size BIGINT,
                    file_format TEXT,
                    row_count INTEGER,
                    last_modified TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bucket_name, object_name)
                )
            """)
            self.pg_conn.commit()
            logger.info("Created data catalog table")
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Error creating catalog: {e}")
        finally:
            cursor.close()
    
    # testing only
    def run_complete_pipeline(self):
        """Execute the full ETL pipeline"""
        logger.info("🚀 Starting Lakehouse ETL Pipeline")
        
        # Setup infrastructure
        self.ensure_bucket_exists()
        self.create_data_catalog()
        
        # Upload sample data to MinIO
        sample_files = [
            ('/sample-data/products.csv', 'raw/products.csv'),
            ('/sample-data/sales.csv', 'raw/sales.csv'),
            ('/sample-data/customers.csv', 'raw/customers.csv')
        ]
        
        for local_path, minio_path in sample_files:
            if os.path.exists(local_path):
                self.minio_client.fput_object(
                    self.bucket_name, minio_path, local_path
                )
                logger.info(f"📤 Uploaded {minio_path}")
        
        # ETL Process: Extract → Transform → Load
        tables = {
            'raw/products.csv': 'products_warehouse',
            'raw/sales.csv': 'sales_warehouse',
            'raw/customers.csv': 'customers_warehouse'
        }
        
        for object_name, table_name in tables.items():
            # Extract from MinIO
            df = self.read_csv_from_minio(object_name)
            
            if df is not None:
                # Load to PostgreSQL
                self.load_dataframe_to_postgres(df, table_name)
                
                # Save as Parquet for analytics
                parquet_name = object_name.replace('raw/', 'processed/').replace('.csv', '.parquet')
                self.write_parquet_to_minio(df, parquet_name)
        
        logger.info("✅ Pipeline completed successfully!")
    
    def close(self):
        """Cleanup connections"""
        if self.pg_conn:
            self.pg_conn.close()

def run_pipeline_for_object(object_name: str):
    """
    Called by Airflow.
    Downloads raw file from MinIO,
    auto-detects type,
    runs appropriate pipeline,
    uploads processed output,
    updates catalog.
    """
    from pipelines import structured_pipeline, pdf_pipeline, docx_pipeline, image_pipeline, ppt_pipeline

    etl = LakehouseETL()

    ext = object_name.rsplit('.', 1)[-1].lower()

    if ext in ("csv", "json", "parquet"):
        structured_pipeline.process_minio_object(
            etl.minio_client, etl.bucket_name, object_name, etl.pg_conn, etl.update_catalog
        )
    elif ext == "pdf":
        pdf_pipeline.process_minio_object(
            etl.minio_client, etl.bucket_name, object_name, etl.pg_conn, etl.update_catalog
        )
    elif ext in ("doc", "docx"):
        docx_pipeline.process_minio_object(
            etl.minio_client, etl.bucket_name, object_name, etl.pg_conn, etl.update_catalog
        )
    elif ext in ("png", "jpg", "jpeg", "tiff"):
        image_pipeline.process_minio_object(
            etl.minio_client, etl.bucket_name, object_name, etl.pg_conn, etl.update_catalog
        )
    elif ext in ("ppt", "pptx"):
        ppt_pipeline.process_minio_object(
            etl.minio_client, etl.bucket_name, object_name, etl.pg_conn, etl.update_catalog
        )
    else:
        logger.error(f"Unsupported format: {ext}")
        return

    logger.info(f"Processing complete for: {object_name}")
    etl.pg_conn.commit()
    etl.pg_conn.close()


if __name__ == "__main__":
    import time
    time.sleep(15)  # Wait for services
    
    etl = LakehouseETL()
    try:
        etl.run_complete_pipeline()
    finally:
        etl.close()