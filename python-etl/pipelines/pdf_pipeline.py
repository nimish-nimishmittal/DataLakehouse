# python-etl/pipelines/pdf_pipeline.py

import io
import logging
import hashlib
from typing import Optional

from minio import Minio
from pypdf import PdfReader
import pdfplumber
import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def calculate_file_hash(data: bytes) -> str:
    """Return SHA-256 hash of a bytes buffer."""
    return hashlib.sha256(data).hexdigest()


def is_duplicate(pg_conn, content_hash: str) -> bool:
    """
    Check in minio_data_catalog if this hash already exists.
    If yes, we can skip heavy processing.
    """
    if pg_conn is None:
        return False

    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            SELECT 1 FROM minio_data_catalog
            WHERE content_hash = %s
            LIMIT 1
            """,
            (content_hash,),
        )
        return cursor.fetchone() is not None
    except Exception as e:
        logger.warning(f"[pdf] Error checking duplicate: {e}")
        return False
    finally:
        cursor.close()


def _ensure_unstructured_table(pg_conn):
    """
    Make sure unstructured_documents table exists with all required columns.
    Uses a safe creation approach that handles existing tables.
    """
    cursor = pg_conn.cursor()
    try:
        # First, create the table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS unstructured_documents (
                id SERIAL PRIMARY KEY,
                object_name TEXT NOT NULL,
                file_type TEXT,
                text_content TEXT,
                content_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        
        # Try to add UNIQUE constraint if it doesn't exist
        # This will fail silently if the constraint already exists
        try:
            cursor.execute(
                """
                ALTER TABLE unstructured_documents 
                ADD CONSTRAINT unstructured_documents_content_hash_key 
                UNIQUE (content_hash)
                """
            )
            logger.info("[pdf] Added UNIQUE constraint to content_hash")
        except psycopg2.errors.DuplicateTable:
            # Constraint already exists, that's fine
            pg_conn.rollback()
            logger.debug("[pdf] UNIQUE constraint already exists on content_hash")
        except Exception as e:
            # Log but continue - we'll handle conflicts differently
            pg_conn.rollback()
            logger.warning(f"[pdf] Could not add UNIQUE constraint: {e}")
        
        pg_conn.commit()
        
    except Exception as e:
        pg_conn.rollback()
        logger.exception(f"[pdf] Failed ensuring unstructured_documents table: {e}")
        raise
    finally:
        cursor.close()


def _save_unstructured_doc(
    pg_conn,
    object_name: str,
    file_type: str,
    text: str,
    content_hash: str,
):
    """
    Save raw extracted text + hash into unstructured_documents.
    Uses a safe upsert approach that works with or without UNIQUE constraint.
    """
    if not pg_conn:
        logger.warning("[pdf] No database connection, skipping unstructured doc save")
        return
    
    _ensure_unstructured_table(pg_conn)
    cursor = pg_conn.cursor()
    
    try:
        # First, try to check if this hash already exists
        cursor.execute(
            """
            SELECT id FROM unstructured_documents 
            WHERE content_hash = %s 
            LIMIT 1
            """,
            (content_hash,)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing record
            cursor.execute(
                """
                UPDATE unstructured_documents
                SET text_content = %s,
                    object_name = %s,
                    file_type = %s,
                    created_at = CURRENT_TIMESTAMP
                WHERE content_hash = %s
                """,
                (text, object_name, file_type, content_hash),
            )
            logger.info(f"[pdf] Updated existing record in unstructured_documents for {object_name}")
        else:
            # Insert new record
            cursor.execute(
                """
                INSERT INTO unstructured_documents 
                    (object_name, file_type, text_content, content_hash)
                VALUES (%s, %s, %s, %s)
                """,
                (object_name, file_type, text, content_hash),
            )
            logger.info(f"[pdf] Inserted new record into unstructured_documents for {object_name}")
        
        pg_conn.commit()
        
    except Exception as e:
        pg_conn.rollback()
        logger.exception(f"[pdf] Failed saving to unstructured_documents: {e}")
        # Don't raise - allow pipeline to continue even if this fails
        logger.warning("[pdf] Continuing pipeline despite unstructured_documents save failure")
    finally:
        cursor.close()


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame for PDF table extraction.
    Handles common issues in PDF-extracted tables.
    """
    # Convert all column names to strings and sanitize
    df.columns = [str(col).strip() if col is not None else f"column_{i}" 
                  for i, col in enumerate(df.columns)]
    
    # Remove completely empty rows and columns
    df = df.dropna(how='all', axis=0)  # Remove empty rows
    df = df.dropna(how='all', axis=1)  # Remove empty columns
    
    # Replace empty strings with None
    df = df.replace('', None)
    df = df.replace(r'^\s*$', None, regex=True)
    
    # Strip whitespace from all string values
    for col in df.select_dtypes(include=['object']).columns:
        try:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception as e:
            logger.warning(f"[pdf] Could not strip whitespace from column {col}: {e}")
    
    return df


def _process_extracted_table(
    minio_client,
    bucket_name: str,
    table_df: pd.DataFrame,
    table_key: str,
    pg_conn,
    catalog_updater,
):
    """
    Process a single extracted table from PDF:
    1. Normalize and clean the DataFrame
    2. Upload CSV to MinIO
    3. Create PostgreSQL table
    4. Update catalog
    
    This function is fault-tolerant and logs errors without failing the entire pipeline.
    """
    try:
        # 0. Normalize the dataframe
        table_df = _normalize_dataframe(table_df)
        
        # Check if dataframe is empty after normalization
        if table_df.empty or len(table_df.columns) == 0:
            logger.warning(f"[pdf] Table is empty after normalization, skipping: {table_key}")
            return
        
        # 1. Upload CSV to MinIO
        csv_bytes = table_df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name,
            table_key,
            io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )
        logger.info(f"[pdf] Uploaded table CSV -> {table_key}")

        # 2. Import from structured_pipeline for consistency
        try:
            from pipelines.structured_pipeline import (
                sanitize_table_name, 
                sanitize_column_name, 
                infer_postgres_type,
                normalize_dataframe as normalize_for_postgres
            )
        except ImportError as e:
            logger.error(f"[pdf] Failed to import from structured_pipeline: {e}")
            logger.warning(f"[pdf] Skipping PostgreSQL table creation for {table_key}")
            return
        
        # 3. Prepare for PostgreSQL
        table_name = sanitize_table_name(table_key)
        
        # Sanitize column names
        table_df.columns = [sanitize_column_name(col) for col in table_df.columns]
        
        # Handle duplicate column names
        cols = pd.Series(table_df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
        table_df.columns = cols
        
        # Normalize for PostgreSQL (flatten nested structures if any)
        table_df = normalize_for_postgres(table_df)
        
        # 4. Load to PostgreSQL
        cursor = pg_conn.cursor()
        
        try:
            # Drop existing table
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            
            # Create table with inferred types
            column_defs = []
            for col in table_df.columns:
                try:
                    pg_type = infer_postgres_type(table_df[col])
                except Exception as type_error:
                    logger.warning(f"[pdf] Failed to infer type for column {col}: {type_error}. Using TEXT.")
                    pg_type = 'TEXT'
                column_defs.append(f'"{col}" {pg_type}')
            
            create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
            cursor.execute(create_sql)
            logger.info(f"[pdf] Created table: {table_name}")
            
            # Bulk insert
            buffer = io.StringIO()
            table_df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            cursor.copy_expert(
                f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')',
                buffer
            )
            
            pg_conn.commit()
            logger.info(f"[pdf] Loaded {len(table_df)} rows into {table_name}")
            
        except Exception as db_error:
            pg_conn.rollback()
            logger.exception(f"[pdf] Database operation failed for {table_name}: {db_error}")
            raise
        finally:
            cursor.close()

        # 5. Update catalog for the extracted table CSV
        try:
            catalog_updater(
                object_name=table_key,
                object_size=len(csv_bytes),
                file_format='csv',
                row_count=len(table_df),
                text_extracted=False,
                content_hash=None
            )
        except Exception as catalog_error:
            logger.warning(f"[pdf] Failed to update catalog for {table_key}: {catalog_error}")

    except Exception as e:
        logger.exception(f"[pdf] Failed processing table {table_key}: {e}")
        if pg_conn:
            pg_conn.rollback()
        # Don't raise - allow other tables to be processed


def process_minio_object(
    minio_client: Minio,
    bucket_name: str,
    object_name: str,
    pg_conn,
    catalog_updater,
):
    """
    Main entry point for PDF processing.

    Steps:
    1. Downloads raw PDF from MinIO
    2. Computes SHA256 hash and checks for duplicates
    3. Extracts plain text with PyPDF
    4. Saves text to:
       - MinIO: processed/unstructured/text-extracted/<file>.txt
       - Postgres: unstructured_documents table
    5. Extracts tables with pdfplumber:
       - Uploads each as CSV to processed/structured/pdf-tables/<file>_table_X.csv
       - Creates PostgreSQL table for each
       - Updates catalog for each table
    6. Updates catalog for original PDF file
    
    This pipeline is fault-tolerant and continues even if individual steps fail.
    """
    logger.info(f"[pdf] Processing {object_name}")
    
    response = None
    data = None
    
    try:
        # 1. Download from MinIO
        response = minio_client.get_object(bucket_name, object_name)
        data = response.read()
        
    except Exception as e:
        logger.exception(f"[pdf] Failed to download {object_name} from MinIO: {e}")
        raise
    finally:
        if response:
            response.close()
            response.release_conn()

    if not data:
        logger.error(f"[pdf] No data downloaded for {object_name}")
        return

    file_size = len(data)
    file_hash = calculate_file_hash(data)
    file_root = object_name.split("/")[-1].rsplit(".", 1)[0]

    # 2. Check for duplicates
    if is_duplicate(pg_conn, file_hash):
        logger.info(
            f"[pdf] Duplicate detected via content_hash, "
            f"skipping heavy processing: {object_name}"
        )
        # Still update catalog with basic info
        try:
            catalog_updater(
                object_name=object_name,
                object_size=file_size,
                file_format="pdf",
                row_count=0,
                text_extracted=False,
                content_hash=file_hash,
            )
        except Exception as e:
            logger.warning(f"[pdf] Failed catalog update for duplicate: {e}")
        return

    # 3. Extract text using pypdf
    logger.info(f"[pdf] Extracting text from PDF...")
    full_text = ""
    page_count = 0
    
    try:
        reader = PdfReader(io.BytesIO(data))
        page_count = len(reader.pages)
        pdf_reader = PdfReader(io.BytesIO(data))
        pdf_meta = pdf_reader.metadata or {}
        pdf_metadata = {
            'author': pdf_meta.get('/Author'),
            'creator': pdf_meta.get('/Creator'),
            'producer': pdf_meta.get('/Producer'),
            'subject': pdf_meta.get('/Subject'),
            'title': pdf_meta.get('/Title'),
            'creation_date': pdf_meta.get('/CreationDate'),
            'page_count': page_count,
            'table_count': table_count
        }
        
        for page_num, page in enumerate(reader.pages, 1):
            try:
                page_text = page.extract_text() or ""
                full_text += page_text
                if page_num % 10 == 0:
                    logger.info(f"[pdf] Processed {page_num}/{page_count} pages")
            except Exception as e:
                logger.warning(f"[pdf] Failed to extract text from page {page_num}: {e}")
        
        logger.info(f"[pdf] Extracted {len(full_text)} characters of text from {page_count} pages")
        
    except Exception as e:
        logger.exception(f"[pdf] Text extraction failed for {object_name}: {e}")
        # Continue with table extraction even if text extraction fails

    # 4. Upload text to MinIO and save to database
    if full_text.strip():
        try:
            text_bytes = full_text.encode("utf-8")
            text_path = f"processed/unstructured/text-extracted/{file_root}.txt"

            minio_client.put_object(
                bucket_name,
                text_path,
                io.BytesIO(text_bytes),
                length=len(text_bytes),
                content_type="text/plain",
            )
            logger.info(f"[pdf] Uploaded extracted text -> {text_path}")

        except Exception as e:
            logger.exception(f"[pdf] Failed to upload text to MinIO: {e}")

        # 5. Save unstructured text to Postgres
        if pg_conn is not None:
            try:
                _save_unstructured_doc(
                    pg_conn=pg_conn,
                    object_name=object_name,
                    file_type="pdf",
                    text=full_text,
                    content_hash=file_hash,
                )
            except Exception as e:
                logger.warning(f"[pdf] Failed to save unstructured doc: {e}")
                # Continue pipeline even if this fails
    else:
        logger.warning(f"[pdf] No text extracted from {object_name}")

    # 6. Extract tables using pdfplumber
    logger.info(f"[pdf] Extracting tables from PDF...")
    table_count = 0
    total_rows = 0
    
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page_idx, page in enumerate(pdf.pages):
                try:
                    tables = page.extract_tables()
                    
                    if not tables:
                        continue
                    
                    for table_idx, table in enumerate(tables):
                        if not table or len(table) < 2:
                            logger.debug(f"[pdf] Skipping empty/invalid table on page {page_idx + 1}")
                            continue

                        # Convert to DataFrame (first row as header)
                        try:
                            # Check if we have valid headers
                            headers = table[0]
                            if not headers or all(h is None or str(h).strip() == '' for h in headers):
                                logger.warning(f"[pdf] Invalid headers on page {page_idx + 1}, table {table_idx + 1}")
                                # Use generic column names
                                headers = [f"column_{i}" for i in range(len(table[0]))]
                            
                            df = pd.DataFrame(table[1:], columns=headers)
                            
                            # Quick validation
                            if df.empty or len(df.columns) == 0:
                                continue
                            
                            # Generate unique table key
                            table_key = (
                                f"processed/structured/pdf-tables/"
                                f"{file_root}_page{page_idx + 1}_table{table_idx + 1}.csv"
                            )

                            # Process this table (fault-tolerant)
                            _process_extracted_table(
                                minio_client=minio_client,
                                bucket_name=bucket_name,
                                table_df=df,
                                table_key=table_key,
                                pg_conn=pg_conn,
                                catalog_updater=catalog_updater,
                            )

                            table_count += 1
                            total_rows += len(df)
                            logger.info(
                                f"[pdf] Processed table {table_count}: "
                                f"{len(df)} rows, {len(df.columns)} columns"
                            )
                            
                        except Exception as table_error:
                            logger.warning(
                                f"[pdf] Failed to process table {table_idx + 1} "
                                f"on page {page_idx + 1}: {table_error}"
                            )
                            
                except Exception as page_error:
                    logger.warning(f"[pdf] Error processing page {page_idx + 1}: {page_error}")
                    
    except Exception as e:
        logger.exception(f"[pdf] Error during table extraction (pdfplumber): {e}")

    logger.info(f"[pdf] Extracted {table_count} tables with {total_rows} total rows")

    # 7. Update catalog for original PDF file
    try:
        catalog_updater(
        object_name=object_name,
        object_size=file_size,
        file_format="pdf",
        row_count=table_count,
        text_extracted=bool(full_text.strip()),
        content_hash=file_hash,
        metadata=pdf_metadata  # NEW
    )
        logger.info(f"[pdf] Updated catalog for {object_name}")
    except Exception as e:
        logger.exception(f"[pdf] Failed catalog update for {object_name}: {e}")
        # Don't raise - pipeline has done its work

    logger.info(
        f"[pdf] ✅ Completed processing: {object_name} | "
        f"pages={page_count} | tables={table_count} | rows={total_rows} | "
        f"text_chars={len(full_text)}"
    )
