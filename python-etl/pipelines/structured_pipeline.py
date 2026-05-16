# python-etl/pipelines/structured_pipeline.py

import io
import csv
import json
import pandas as pd
import chardet
from psycopg2.extras import execute_values
import logging
import re
from typing import Any, List, Dict
from minio import Minio

logger = logging.getLogger(__name__)

def detect_encoding(byte_data, sample_size=10000):
    """Detect file encoding using chardet"""
    sample = byte_data[:sample_size]
    result = chardet.detect(sample)
    encoding = result.get('encoding', 'utf-8')
    confidence = result.get('confidence', 0)
    logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
    
    # Fallback encodings if confidence is low
    if confidence < 0.7:
        fallback_encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        logger.warning(f"Low confidence encoding detection. Will try fallbacks: {fallback_encodings}")
        return fallback_encodings
    
    return [encoding] if encoding else ['utf-8']


def detect_delimiter(text_sample, max_sample=5000):
    """Enhanced delimiter detection with multiple strategies"""
    sample = text_sample[:max_sample]
    
    # Try csv.Sniffer first
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
        delimiter = dialect.delimiter
        logger.info(f"Sniffer detected delimiter: {repr(delimiter)}")
        return delimiter
    except Exception as e:
        logger.warning(f"Sniffer failed: {e}. Using fallback detection.")
    
    # Fallback: count occurrences of common delimiters
    delimiters = [',', ';', '\t', '|', ':']
    lines = sample.split('\n')[:10]
    
    if not lines:
        return ','
    
    delimiter_counts = {d: [] for d in delimiters}
    
    for line in lines:
        if line.strip():
            for delim in delimiters:
                delimiter_counts[delim].append(line.count(delim))
    
    # Find delimiter with most consistent count across lines
    best_delimiter = ','
    best_score = 0
    
    for delim, counts in delimiter_counts.items():
        if not counts or all(c == 0 for c in counts):
            continue
        
        avg_count = sum(counts) / len(counts)
        if avg_count > 0:
            variance = sum((c - avg_count) ** 2 for c in counts) / len(counts)
            score = avg_count / (1 + variance)
            
            if score > best_score:
                best_score = score
                best_delimiter = delim
    
    logger.info(f"Fallback detected delimiter: {repr(best_delimiter)}")
    return best_delimiter


def sanitize_column_name(col):
    """Sanitize column names for PostgreSQL compatibility"""
    col = str(col).strip()
    col = re.sub(r'[^\w\s]', '_', col)
    col = re.sub(r'\s+', '_', col)
    col = col.strip('_')
    
    if col and col[0].isdigit():
        col = 'col_' + col
    if not col:
        col = 'unnamed_column'
    
    col = col.lower()
    return col


def sanitize_table_name(object_name):
    """Create valid PostgreSQL table name from object path"""
    filename = object_name.split('/')[-1]
    table_name = filename.rsplit('.', 1)[0]
    table_name = re.sub(r'[^\w]', '_', table_name)
    table_name = table_name.strip('_').lower()
    table_name = f"data_{table_name}"
    
    if len(table_name) > 63:
        table_name = table_name[:63]
    
    logger.info(f"Table name: {table_name}")
    return table_name


def flatten_json_value(value: Any) -> str:
    """Convert complex JSON values (dict, list) to JSON strings"""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame by flattening nested structures.
    Converts dicts/lists to JSON strings for PostgreSQL compatibility.
    """
    logger.info(f"[Normalize] Normalizing DataFrame with {len(df.columns)} columns")
    
    for col in df.columns:
        # Check if column contains complex types
        sample = df[col].dropna().head(10)
        
        if len(sample) > 0:
            has_complex = any(isinstance(val, (dict, list)) for val in sample)
            
            if has_complex:
                logger.info(f"[Normalize] Flattening complex column: {col}")
                df[col] = df[col].apply(flatten_json_value)
    
    return df


def infer_postgres_type(series: pd.Series) -> str:
    """
    Infer PostgreSQL data type from a pandas Series.

    Order of checks is intentional:
      1. Empty / all-null  → TEXT  (safe default)
      2. Already-complex   → JSONB
      3. JSON strings      → JSONB
      4. Boolean strings   → BOOLEAN  ← MUST come before numeric so that
                                         "True"/"False"/"yes"/"no" are never
                                         misidentified as INTEGER and then
                                         crash PostgreSQL COPY.
      5. Numeric           → INTEGER / BIGINT / NUMERIC
      6. Datetime          → TIMESTAMP
      7. Fallback          → TEXT
    """
    if len(series) == 0 or series.isna().all():
        return 'TEXT'

    sample = series.dropna()
    if len(sample) == 0:
        return 'TEXT'

    first_val = sample.iloc[0]

    # ── 1. Already-complex Python types (after json_normalize) ───
    if isinstance(first_val, (dict, list)):
        return 'JSONB'

    # ── 2. JSON-string detection ──────────────────────────────────
    try:
        if sample.dtype == 'object':
            sample_vals = sample.head(5)
            json_like = sum(
                1 for v in sample_vals
                if isinstance(v, str)
                and (v.strip().startswith('{') or v.strip().startswith('['))
            )
            if json_like >= len(sample_vals) * 0.8:
                return 'JSONB'
    except Exception:
        pass

    # ── 3. Boolean detection — BEFORE numeric ────────────────────
    # Covers: True/False, true/false, TRUE/FALSE, yes/no, t/f
    # Intentionally excludes 1/0 — those stay as INTEGER.
    try:
        str_bool_values = {'true', 'false', 'yes', 'no', 't', 'f'}
        unique_lower = set(str(v).strip().lower() for v in sample.unique()[:20])
        if unique_lower and unique_lower.issubset(str_bool_values) and len(unique_lower) <= 2:
            return 'BOOLEAN'
    except Exception:
        pass

    # ── 4. Numeric detection ──────────────────────────────────────
    try:
        numeric_series = pd.to_numeric(sample, errors='raise')
        if (numeric_series % 1 == 0).all():
            min_val = numeric_series.min()
            max_val = numeric_series.max()
            if min_val >= -2147483648 and max_val <= 2147483647:
                return 'INTEGER'
            else:
                return 'BIGINT'
        else:
            return 'NUMERIC'
    except (ValueError, TypeError):
        pass

    # ── 5. Datetime detection ─────────────────────────────────────
    try:
        pd.to_datetime(sample.head(10), errors='raise')
        return 'TIMESTAMP'
    except Exception:
        pass

    return 'TEXT'


def detect_file_type(object_name, data):
    """Detect file type from extension and content"""
    ext = object_name.lower().rsplit('.', 1)[-1] if '.' in object_name else ''
    
    if ext in ['csv', 'tsv', 'txt']:
        return 'csv'
    elif ext == 'json':
        return 'json'
    elif ext in ['parquet', 'pq']:
        return 'parquet'
    
    # Try to detect from content
    try:
        text = data[:1000].decode('utf-8', errors='ignore')
        json.loads(text)
        return 'json'
    except:
        pass
    
    return 'csv'


def read_csv_file(data, encodings):
    """Read CSV file with multiple encoding and delimiter attempts"""
    df = None
    last_error = None
    
    for encoding in encodings:
        try:
            logger.info(f"[CSV] Attempting to decode with {encoding}")
            text = data.decode(encoding)
            delimiter = detect_delimiter(text)
            
            read_attempts = [
                {
                    'sep': delimiter,
                    'engine': 'python',
                    'encoding': encoding,
                    'on_bad_lines': 'skip',
                    'quoting': csv.QUOTE_MINIMAL
                },
                {
                    'sep': delimiter,
                    'engine': 'python',
                    'encoding': encoding,
                    'on_bad_lines': 'skip',
                    'quoting': csv.QUOTE_ALL,
                    'escapechar': '\\'
                },
                {
                    'sep': delimiter,
                    'engine': 'python',
                    'encoding': encoding,
                    'on_bad_lines': 'skip',
                    'quoting': csv.QUOTE_NONE
                }
            ]
            
            for attempt_num, read_params in enumerate(read_attempts, 1):
                try:
                    logger.info(f"[CSV] Read attempt {attempt_num}")
                    df = pd.read_csv(io.BytesIO(data), **read_params)
                    
                    if df is not None and not df.empty:
                        logger.info(f"[CSV] Successfully read {len(df)} rows with {len(df.columns)} columns")
                        return df
                except Exception as attempt_error:
                    logger.warning(f"[CSV] Attempt {attempt_num} failed: {attempt_error}")
                    last_error = attempt_error
                    continue
                    
        except Exception as encoding_error:
            logger.warning(f"[CSV] Encoding {encoding} failed: {encoding_error}")
            last_error = encoding_error
            continue
    
    if df is None or (df is not None and df.empty):
        raise ValueError(f"Failed to read CSV. Last error: {last_error}")
    
    return df


def read_json_file(data, encodings):
    """Read JSON file - handles arrays, objects, and newline-delimited JSON"""
    df = None
    last_error = None
    
    for encoding in encodings:
        try:
            logger.info(f"[JSON] Attempting to decode with {encoding}")
            text = data.decode(encoding)
            
            # Try standard JSON first
            try:
                json_data = json.loads(text)
                
                # Handle single object - wrap in list
                if isinstance(json_data, dict):
                    logger.info(f"[JSON] Converting single object to array")
                    json_data = [json_data]
                
                # Handle array of objects
                if isinstance(json_data, list):
                    df = pd.json_normalize(json_data)  # Use json_normalize for nested structures
                    logger.info(f"[JSON] Successfully read as standard JSON: {len(df)} rows")
                    return df
                else:
                    raise ValueError(f"Unsupported JSON structure: {type(json_data)}")
                    
            except json.JSONDecodeError:
                # Try newline-delimited JSON (JSONL/NDJSON)
                logger.info(f"[JSON] Trying newline-delimited JSON format")
                lines = text.strip().split('\n')
                records = []
                
                for i, line in enumerate(lines):
                    if line.strip():
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"[JSON] Skipping invalid JSON at line {i+1}: {e}")
                            continue
                
                if records:
                    df = pd.json_normalize(records)  # Use json_normalize for nested structures
                    logger.info(f"[JSON] Successfully read as JSONL: {len(df)} rows")
                    return df
                else:
                    raise ValueError("No valid JSON records found")
                    
        except Exception as e:
            logger.warning(f"[JSON] Encoding {encoding} failed: {e}")
            last_error = e
            continue
    
    if df is None:
        raise ValueError(f"Failed to read JSON. Last error: {last_error}")
    
    return df


def read_parquet_file(data):
    """Read Parquet file"""
    try:
        logger.info(f"[Parquet] Reading parquet file")
        df = pd.read_parquet(io.BytesIO(data), engine='pyarrow')
        logger.info(f"[Parquet] Successfully read {len(df)} rows with {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"[Parquet] Failed to read: {e}")
        raise


def clean_dataframe(df):
    """Clean and sanitize the DataFrame"""
    logger.info(f"[Clean] Cleaning dataframe...")
    
    # Sanitize column names
    df.columns = [sanitize_column_name(col) for col in df.columns]
    
    # Handle duplicate column names
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        try:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception as e:
            logger.warning(f"[Clean] Could not strip whitespace from column {col}: {e}")
    
    # Replace various null representations
    null_values = ['nan', 'NaN', 'NA', 'N/A', 'null', 'NULL', 'None', '']
    df = df.replace(null_values, None)
    
    logger.info(f"[Clean] Cleaned dataframe: {len(df)} rows, {len(df.columns)} columns")
    return df


def load_to_postgres(df, table_name, pg_conn, use_smart_types=True):
    """Load DataFrame to PostgreSQL"""
    cursor = pg_conn.cursor()

    try:
        # ── Step 1: flatten nested structures ────────────────────
        df = normalize_dataframe(df)

        # ── Step 2: infer column types ────────────────────────────
        col_types = {}
        column_defs = []

        for col in df.columns:
            if use_smart_types:
                try:
                    pg_type = infer_postgres_type(df[col])
                    logger.info(f"[PostgreSQL] Column '{col}' -> {pg_type}")
                except Exception as e:
                    logger.warning(f"[PostgreSQL] Type inference failed for '{col}': {e}. Using TEXT.")
                    pg_type = 'TEXT'
            else:
                pg_type = 'TEXT'

            col_types[col] = pg_type
            column_defs.append(f'"{col}" {pg_type}')

        # ── Step 3: create table ──────────────────────────────────
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        logger.info(f"[PostgreSQL] Dropped existing table: {table_name}")

        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
        cursor.execute(create_sql)
        logger.info(f"[PostgreSQL] Created table: {table_name}")

        # ── Step 4: prepare COPY-safe export copy ─────────────────
        # PostgreSQL COPY FORMAT CSV is strict about type representations.
        # We normalise every column to the string form PostgreSQL expects:
        #
        #   BOOLEAN  → 't' or 'f'  (NOT 'True'/'False'/'1'/'0')
        #   NUMERIC  → strip trailing whitespace, convert nan → NULL marker
        #   INTEGER  → strip, convert nan → NULL marker
        #   JSONB    → json.dumps() for dicts/lists; leave strings alone
        #   TEXT     → str(), strip; None/NaN → NULL marker
        #   TIMESTAMP→ isoformat string; None/NaN → NULL marker
        #   NULL     → the literal string '\\N' (COPY NULL token)

        NULL_TOKEN = '\\N'

        df_copy = df.copy()

        def _is_null(v):
            if v is None:
                return True
            try:
                return pd.isna(v)
            except Exception:
                return False

        for col in df_copy.columns:
            pg_type = col_types[col]

            if pg_type == 'BOOLEAN':
                _bool_true  = {'true', 'yes', 't', '1'}
                _bool_false = {'false', 'no', 'f', '0'}

                def _to_bool(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    s = str(v).strip().lower()
                    if s in _bool_true:
                        return 't'
                    if s in _bool_false:
                        return 'f'
                    return NULL_TOKEN  # unrecognised value → NULL

                df_copy[col] = df_copy[col].apply(_to_bool)

            elif pg_type in ('INTEGER', 'BIGINT'):
                def _to_int(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    try:
                        return str(int(float(str(v).strip())))
                    except (ValueError, TypeError):
                        return NULL_TOKEN

                df_copy[col] = df_copy[col].apply(_to_int)

            elif pg_type == 'NUMERIC':
                def _to_numeric(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    s = str(v).strip()
                    if s.lower() in ('nan', 'inf', '-inf', ''):
                        return NULL_TOKEN
                    return s

                df_copy[col] = df_copy[col].apply(_to_numeric)

            elif pg_type == 'TIMESTAMP':
                def _to_ts(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    try:
                        return pd.Timestamp(v).isoformat()
                    except Exception:
                        return NULL_TOKEN

                df_copy[col] = df_copy[col].apply(_to_ts)

            elif pg_type == 'JSONB':
                def _to_jsonb(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    if isinstance(v, (dict, list)):
                        return json.dumps(v, ensure_ascii=False)
                    s = str(v).strip()
                    return s if s else NULL_TOKEN

                df_copy[col] = df_copy[col].apply(_to_jsonb)

            else:  # TEXT and anything else
                def _to_text(v):
                    if _is_null(v):
                        return NULL_TOKEN
                    s = str(v).strip()
                    return s if s not in ('nan', 'NaN', 'None') else NULL_TOKEN

                df_copy[col] = df_copy[col].apply(_to_text)

        # ── Step 5: COPY into PostgreSQL ──────────────────────────
        # TEXT format: tab-delimited, \N represents NULL, no quoting needed
        # We must NOT escape backslashes, otherwise \N becomes \\N
        # First sanitize: replace tabs and newlines in data to avoid breaking the format
        for col in df_copy.columns:
            if df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(
                    lambda x: str(x).replace('\t', ' ').replace('\n', ' ').replace('\r', '') if x is not None and x != NULL_TOKEN else x
                )

        buffer = io.StringIO()
        df_copy.to_csv(
            buffer,
            index=False,
            header=False,
            sep='\t',
            na_rep=NULL_TOKEN,
            quoting=csv.QUOTE_NONE,
            escapechar=None,  # Don't escape - let PostgreSQL handle \N as NULL
        )
        buffer.seek(0)

        cursor.copy_expert(
            f"COPY \"{table_name}\" FROM STDIN WITH "
            f"(FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')",
            buffer
        )

        pg_conn.commit()
        logger.info(f"[PostgreSQL] Loaded {len(df)} rows into {table_name}")

    except Exception as e:
        pg_conn.rollback()
        logger.error(f"[PostgreSQL] Load failed: {e}")
        raise
    finally:
        cursor.close()



def save_to_parquet(df, minio_client, bucket_name, object_name):
    """Save DataFrame to Parquet in MinIO"""
    try:
        logger.info(f"[Parquet] Writing Parquet file...")
        
        # Create a copy and ensure compatibility
        df_copy = df.copy()
        
        # Convert any remaining complex types to strings
        for col in df_copy.columns:
            if df_copy[col].dtype == 'object':
                sample = df_copy[col].dropna().head(5)
                if len(sample) > 0 and any(isinstance(v, (dict, list)) for v in sample):
                    logger.info(f"[Parquet] Converting complex column {col} to JSON strings")
                    df_copy[col] = df_copy[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )
        
        parquet_buffer = io.BytesIO()
        df_copy.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)

        parquet_name = object_name.replace('raw/', 'processed/structured/').rsplit('.', 1)[0] + '.parquet'
        minio_client.put_object(
            bucket_name, 
            parquet_name, 
            parquet_buffer, 
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        logger.info(f"[Parquet] Saved to: {parquet_name}")
    except Exception as e:
        logger.error(f"[Parquet] Failed to save: {e}")
        # Don't fail the entire pipeline if parquet save fails
        logger.warning(f"[Parquet] Continuing despite parquet save failure")


def process_minio_object(
        minio_client: Minio, 
        bucket_name: str, 
        object_name: str, 
        pg_conn, 
        catalog_updater,
        uploaded_by=None
        ):
    """
    Main entry point for structured data processing.
    Called by etl_manager.run_pipeline_for_object()
    
    Handles CSV, JSON (including JSONL with nested structures), and Parquet files.
    """
    logger.info(f"[structured] Processing {object_name}")
    resp = None

    try:

        # ---- Fetch uploader identity from MinIO object metadata ---- #
        stat = minio_client.stat_object(bucket_name, object_name)

        uploaded_by = stat.metadata.get("x-amz-meta-uploaded-by")
        uploaded_by = int(uploaded_by) if uploaded_by else None

        # 1. Read raw data from MinIO
        resp = minio_client.get_object(bucket_name, object_name)
        data = resp.read()
        logger.info(f"[structured] Downloaded {len(data)} bytes")

        # 2. Detect file type
        file_type = detect_file_type(object_name, data)
        logger.info(f"[structured] Detected file type: {file_type}")

        # 3. Read file based on type
        df = None
        
        if file_type == 'csv':
            encodings = detect_encoding(data)
            df = read_csv_file(data, encodings)
            
        elif file_type == 'json':
            encodings = detect_encoding(data)
            df = read_json_file(data, encodings)
            
        elif file_type == 'parquet':
            df = read_parquet_file(data)
            
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        # Check if we successfully read the data
        if df is None or df.empty:
            raise ValueError(f"Failed to read file or file is empty")
        
        logger.info(f"[structured] Initial read: {len(df)} rows, {len(df.columns)} columns")
        
        # 4. Clean and sanitize the DataFrame
        df = clean_dataframe(df)

        # 5. Create table name
        table_name = sanitize_table_name(object_name)

        # 6. Load to PostgreSQL
        load_to_postgres(df, table_name, pg_conn, use_smart_types=True)

        # 7. Save as Parquet to MinIO for analytics (skip if already parquet)
        processed_object_name = None
        if file_type != 'parquet':
            save_to_parquet(df, minio_client, bucket_name, object_name)
            # Build the processed path (must match logic in save_to_parquet)
            processed_object_name = object_name.replace('raw/', 'processed/structured/').rsplit('.', 1)[0] + '.parquet'

        # 8. Update catalog with PROCESSED path (not raw path)
        # The file has been processed - catalog should reflect the processed location
        structured_metadata = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'original_path': object_name,  # Track where it came from
            'processed': True,
            'processed_at': __import__('datetime').datetime.utcnow().isoformat()
        }

        # Use processed path if available, otherwise keep original (for parquet files)
        catalog_object_name = processed_object_name if processed_object_name else object_name
        # For parquet input, the processed path is the same as input
        if file_type == 'parquet':
            catalog_object_name = object_name.replace('raw/', 'processed/') if object_name.startswith('raw/') else object_name

        # Update catalog with processed path
        catalog_updater(
            object_name=catalog_object_name,
            object_size=len(data),
            file_format='parquet' if processed_object_name else file_type,  # Mark as parquet if converted
            row_count=len(df),
            text_extracted=False,
            metadata=structured_metadata,
            uploaded_by=uploaded_by
        )

        # IMPORTANT: Remove the old raw/ entry from catalog (it has been processed)
        # Only do this if we actually changed the path
        if object_name != catalog_object_name and object_name.startswith('raw/'):
            try:
                cursor = pg_conn.cursor()
                cursor.execute(
                    "DELETE FROM minio_data_catalog WHERE bucket_name = %s AND object_name = %s",
                    (bucket_name, object_name)
                )
                pg_conn.commit()
                cursor.close()
                logger.info(f"[structured] Removed old catalog entry: {object_name}")
            except Exception as e:
                logger.warning(f"[structured] Failed to remove old raw/ entry: {e}")
                # Don't fail the pipeline if cleanup fails

        logger.info(f"[structured] ✅ Successfully processed {object_name} -> {catalog_object_name}")

    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        logger.exception(f"[structured] ❌ Error processing {object_name}: {str(e)}")
        raise

    finally:
        if resp:
            resp.close()
            resp.release_conn()