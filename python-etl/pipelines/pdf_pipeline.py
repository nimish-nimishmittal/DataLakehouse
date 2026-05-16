# python-etl/pipelines/pdf_pipeline.py

import io
import logging
from typing import Optional
from minio import Minio
from pypdf import PdfReader
import pdfplumber
import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def _ensure_unstructured_table(pg_conn):
    """Ensure unstructured_documents table exists."""
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS unstructured_documents (
                id           SERIAL PRIMARY KEY,
                object_name  TEXT NOT NULL,
                file_type    TEXT,
                text_content TEXT,
                uploaded_by  INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (object_name)
            )
            """
        )
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
    uploaded_by: int = None,
):
    """
    Upsert raw extracted text into unstructured_documents.
    Deduplication is by object_name (filename already guaranteed unique
    by the (1)(2)(3) renaming logic in the uploader).
    """
    if not pg_conn:
        logger.warning("[pdf] No database connection, skipping unstructured doc save")
        return

    _ensure_unstructured_table(pg_conn)
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO unstructured_documents
                (object_name, file_type, text_content, uploaded_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (object_name) DO UPDATE
            SET text_content = EXCLUDED.text_content,
                file_type    = EXCLUDED.file_type,
                uploaded_by  = EXCLUDED.uploaded_by,
                created_at   = CURRENT_TIMESTAMP
            """,
            (object_name, file_type, text, uploaded_by),
        )
        pg_conn.commit()
        logger.info(f"[pdf] Saved unstructured doc for {object_name}")
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




def _extract_tables_rect_based(page) -> list:
    """
    Fallback table extractor for PDFs whose tables have coloured header backgrounds
    but NO border lines around data rows (pdfplumber's default strategy misses data rows
    in this case, returning only the header as a 1-row table).

    Detection logic:
    - Tall rects  (h > 10px) = header cell backgrounds → define column x-boundaries
    - Thin rects  (h <  3px) = separator lines → mark header bottom and table footer
    - Data rows               = plain text words between the two separator groups

    Returns a list of raw tables in pdfplumber format: [[header_row], [data_row], ...]
    so they can be fed directly into the existing DataFrame-building code.
    """
    import pandas as pd

    rects = page.rects
    words = page.extract_words()

    header_rects = [r for r in rects if (r['bottom'] - r['top']) > 10]
    if not header_rects:
        return []

    # Group header rects by top-y into per-table sets
    groups: dict = {}
    for r in header_rects:
        key = round(r['top'])
        groups.setdefault(key, []).append(r)

    sorted_tops = sorted(groups.keys())
    tables = []

    for gi, top_y in enumerate(sorted_tops):
        cols = sorted(groups[top_y], key=lambda r: r['x0'])
        col_bounds = [(c['x0'], c['x1']) for c in cols]
        table_x0   = cols[0]['x0']
        table_x1   = cols[-1]['x1']
        header_top = cols[0]['top']
        header_bottom = max(c['bottom'] for c in cols)
        next_table_top = sorted_tops[gi + 1] if gi + 1 < len(sorted_tops) else page.height

        # Thin rects belonging to this table (search from header_top, not header_bottom,
        # so we include the bottom-border of the header which shares y with header_bottom)
        my_thin = [r for r in rects
                   if (r['bottom'] - r['top']) < 3
                   and r['top'] >= header_top
                   and r['top'] < next_table_top - 10
                   and r['x0'] >= table_x0 - 5
                   and r['x1'] <= table_x1 + 5]

        if not my_thin:
            continue

        # Group thin rects by y into separator lines
        thin_by_y: dict = {}
        for r in my_thin:
            y_key = round(r['top'] * 10) / 10
            thin_by_y.setdefault(y_key, []).append(r)

        # Only consider separators AT OR BELOW header bottom
        below_header = sorted(y for y in thin_by_y if y >= header_bottom - 1)
        if len(below_header) < 2:
            continue  # need at least: one after header, one at table end

        data_start = thin_by_y[below_header[0]][0]['bottom']
        data_end   = thin_by_y[below_header[-1]][0]['top']

        if data_end <= data_start + 5:
            continue

        # --- Extract header text ---
        header_row = []
        for (cx0, cx1) in col_bounds:
            cell_words = [w['text'] for w in words
                          if w['x0'] >= cx0 - 2 and w['x0'] <= cx1 + 2
                          and w['top'] >= header_top - 2
                          and w['bottom'] <= header_bottom + 2]
            header_row.append(' '.join(cell_words))

        # --- Extract data words ---
        data_words = [w for w in words
                      if w['x0'] >= table_x0 - 3 and w['x0'] <= table_x1 + 3
                      and w['top'] >= data_start - 1 and w['top'] <= data_end + 2]

        if not data_words:
            continue

        # Group words into rows by y-position (5pt snap grid)
        rows_by_y: dict = {}
        for w in data_words:
            y_key = round(w['top'] / 5) * 5
            rows_by_y.setdefault(y_key, []).append(w)

        data_rows = []
        for y_key in sorted(rows_by_y.keys()):
            row_words = rows_by_y[y_key]
            row = []
            for (cx0, cx1) in col_bounds:
                cell = ' '.join(
                    w['text'] for w in sorted(row_words, key=lambda w: w['x0'])
                    if w['x0'] >= cx0 - 5 and w['x0'] <= cx1 + 5
                )
                row.append(cell if cell.strip() else None)
            if any(c for c in row if c):
                data_rows.append(row)

        if data_rows:
            # Return in pdfplumber raw format: [header, row1, row2, ...]
            tables.append([header_row] + data_rows)

    return tables

def _process_extracted_table(
    minio_client,
    bucket_name: str,
    object_name: str,
    table_df: pd.DataFrame,
    table_key: str,
    pg_conn,
    catalog_updater,
    uploaded_by: int,
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

                # Validate TIMESTAMP/DATE: infer_postgres_type accepts partial dates
                # like "Dec 2024" but PostgreSQL rejects them during COPY.
                if pg_type in ("TIMESTAMP", "DATE"):
                    try:
                        non_null = table_df[col].dropna()
                        if len(non_null) > 0:
                            pd.to_datetime(non_null, format='%Y-%m-%d', exact=False)
                    except Exception:
                        logger.warning(
                            f"[pdf] Column '{col}' inferred as {pg_type} but values don't "
                            f"parse as full dates. Falling back to TEXT."
                        )
                        pg_type = "TEXT"

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
                uploaded_by=uploaded_by
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
    uploaded_by=None
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

    # ---- Fetch uploader identity from MinIO object metadata ---- #
    stat = minio_client.stat_object(bucket_name, object_name)

    uploaded_by = stat.metadata.get("x-amz-meta-uploaded-by")
    uploaded_by = int(uploaded_by) if uploaded_by else None

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
    file_root = object_name.split("/")[-1].rsplit(".", 1)[0]

    # 2. Extract text using pypdf
    logger.info(f"[pdf] Extracting text from PDF...")
    full_text = ""
    page_count = 0
    
    # pdf_meta_raw holds raw PDF header fields extracted here.
    # pdf_metadata (which includes table_count) is built AFTER table extraction
    # so that table_count is final and accurate — not 0 or unbound.
    pdf_meta_raw = {}
    try:
        reader = PdfReader(io.BytesIO(data))
        page_count = len(reader.pages)
        pdf_meta_raw = dict(reader.metadata or {})

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
                    uploaded_by=uploaded_by
                )
            except Exception as e:
                logger.warning(f"[pdf] Failed to save unstructured doc: {e}")
                # Continue pipeline even if this fails

        # 6. Update catalog for extracted text file
        try:
            catalog_updater(
                object_name=text_path,
                object_size=len(text_bytes),
                file_format="text",
                row_count=None,
                text_extracted=True,
                uploaded_by=uploaded_by,
                metadata={"source": "pdf_text_extraction", "source_pdf": object_name}
            )
            logger.info(f"[pdf] Created catalog entry for extracted text: {text_path}")
        except Exception as catalog_err:
            logger.warning(f"[pdf] Failed to update catalog for extracted text: {catalog_err}")
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
                    # --- Primary extraction: pdfplumber line/rect strategy ---
                    tables = page.extract_tables() or []

                    # --- Fallback: rect-based extractor for styled PDFs ---
                    # If pdfplumber only returned header-only rows (all len==1),
                    # the PDF likely uses coloured header backgrounds with no row
                    # borders. Switch to our geometry-aware extractor.
                    all_header_only = tables and all(len(t) < 2 for t in tables)
                    if not tables or all_header_only:
                        fallback = _extract_tables_rect_based(page)
                        if fallback:
                            logger.info(
                                f"[pdf] Page {page_idx + 1}: pdfplumber found "
                                f"{len(tables)} header-only table(s); "
                                f"rect-based fallback found {len(fallback)} table(s)"
                            )
                            tables = fallback
                        elif not tables:
                            continue

                    for table_idx, table in enumerate(tables):
                        if not table or len(table) < 2:
                            logger.debug(f"[pdf] Skipping empty/invalid table on page {page_idx + 1}")
                            continue

                        try:
                            headers = table[0]
                            if not headers or all(h is None or str(h).strip() == '' for h in headers):
                                logger.warning(f"[pdf] Invalid headers on page {page_idx + 1}, table {table_idx + 1}")
                                headers = [f"column_{i}" for i in range(len(table[0]))]

                            df = pd.DataFrame(table[1:], columns=headers)

                            if df.empty or len(df.columns) == 0:
                                continue

                            table_key = (
                                f"processed/structured/pdf-tables/"
                                f"{file_root}_page{page_idx + 1}_table{table_idx + 1}.csv"
                            )

                            _process_extracted_table(
                                minio_client=minio_client,
                                bucket_name=bucket_name,
                                object_name=object_name,
                                table_df=df,
                                table_key=table_key,
                                pg_conn=pg_conn,
                                catalog_updater=catalog_updater,
                                uploaded_by=uploaded_by,
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

    # Build pdf_metadata here — table_count is now final and accurate.
    # pdf_meta_raw was populated during text extraction above (empty dict if that failed).
    pdf_metadata = {
        'author':        pdf_meta_raw.get('/Author'),
        'creator':       pdf_meta_raw.get('/Creator'),
        'producer':      pdf_meta_raw.get('/Producer'),
        'subject':       pdf_meta_raw.get('/Subject'),
        'title':         pdf_meta_raw.get('/Title'),
        'creation_date': pdf_meta_raw.get('/CreationDate'),
        'page_count':    page_count,
        'table_count':   table_count,
        'uploaded_by':   uploaded_by,
    }

    # 7. Update catalog for the original PDF file (keep reference to extracted assets)
    # The original PDF remains in the raw-data bucket; processed outputs are:
    #   - extracted text: processed/unstructured/text-extracted/<file>.txt
    #   - extracted tables: processed/structured/pdf-tables/<file>_pageX_tableY.csv
    try:
        catalog_updater(
            object_name=object_name,
            object_size=file_size,
            file_format="pdf",
            row_count=table_count,
            text_extracted=True,
            metadata={
                **pdf_metadata,
                "status": "processed",
                "extracted_text": f"processed/unstructured/text-extracted/{root_name}.txt",
                "extracted_tables_prefix": f"processed/structured/pdf-tables/{root_name}_",
            },
            uploaded_by=uploaded_by,
        )
        logger.info(f"[pdf] Updated catalog entry for PDF: {object_name}")
    except Exception as e:
        logger.exception(f"[pdf] Failed catalog update for {object_name}: {e}")
        # Don't raise - pipeline has done its work

    # 8. Remove the raw/ catalog entry (file has been processed)
    if object_name.startswith("raw/"):
        try:
            cursor = pg_conn.cursor()
            cursor.execute(
                "DELETE FROM minio_data_catalog WHERE bucket_name = %s AND object_name = %s",
                (bucket_name, object_name)
            )
            pg_conn.commit()
            cursor.close()
            logger.info(f"[pdf] Removed old catalog entry: {object_name}")
        except Exception as delete_err:
            logger.warning(f"[pdf] Failed to remove old raw/ entry: {delete_err}")

    logger.info(
        f"[pdf] ✅ Completed processing: {object_name} | "
        f"pages={page_count} | tables={table_count} | rows={total_rows} | "
        f"text_chars={len(full_text)}"
    )