# python-etl/pipelines/docx_pipeline.py

import io
import logging
from typing import List, Optional

import psycopg2
from minio import Minio
import pandas as pd
from docx import Document   # python-docx

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# helpers: hashing & de-dup
# ─────────────────────────────────────────────────────────────

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
    except Exception:
        pg_conn.rollback()
        logger.exception("[docx] Failed ensuring unstructured_documents table")
        raise
    finally:
        cursor.close()


def _save_unstructured_doc(
    pg_conn,
    object_name: str,
    file_type: str,
    text_content: str,
    uploaded_by: Optional[int],
):
    """
    Upsert extracted text into unstructured_documents keyed by object_name.
    Deduplication is by object_name — filenames are already unique at upload time.
    """
    if not pg_conn:
        logger.warning("[docx] No DB connection — skipping unstructured_documents save")
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
            (object_name, file_type, text_content, uploaded_by),
        )
        pg_conn.commit()
        logger.info(f"[docx] Saved unstructured doc for {object_name}")
    except Exception as e:
        pg_conn.rollback()
        logger.exception(f"[docx] Failed saving to unstructured_documents: {e}")
    finally:
        cursor.close()




# ─────────────────────────────────────────────────────────────
# helpers: text & table extraction from DOCX bytes
# ─────────────────────────────────────────────────────────────

def _extract_text_from_docx_bytes(data: bytes) -> str:
    """Extract plain paragraph text from DOCX bytes using python-docx."""
    doc = Document(io.BytesIO(data))
    lines = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    return "\n".join(lines)


def _extract_tables_from_docx_bytes(data: bytes) -> List[pd.DataFrame]:
    """
    Extract all tables from DOCX as a list of pandas DataFrames.
    First row is used as header when non-empty; generic column names otherwise.
    """
    doc = Document(io.BytesIO(data))
    dfs: List[pd.DataFrame] = []

    for tbl in doc.tables:
        if not tbl.rows:
            continue

        max_cols = max((len(r.cells) for r in tbl.rows), default=0)
        if max_cols == 0:
            continue

        rows = []
        for row in tbl.rows:
            cells = [c.text.strip() for c in row.cells]
            # Pad short rows so every row has the same width
            if len(cells) < max_cols:
                cells += [""] * (max_cols - len(cells))
            rows.append(cells)

        if not rows:
            continue

        # Build column names from the first row
        header = rows[0]
        data_rows = rows[1:]

        cols: List[str] = []
        seen: dict = {}
        for col in header:
            name = col.strip() or "col"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 1
            cols.append(name)

        df = pd.DataFrame(data_rows, columns=cols) if data_rows else pd.DataFrame(columns=cols)
        df = df.dropna(how="all")
        df = df.replace("", None)

        if not df.empty:
            dfs.append(df)

    return dfs


# ─────────────────────────────────────────────────────────────
# helper: normalize DataFrame before writing to PostgreSQL

def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a DataFrame coming out of DOCX table extraction.
    Mirrors the same function in pdf_pipeline:
      - All column names are strings
      - Fully-empty rows AND columns are dropped
      - Blank strings become None
      - String cell values are stripped of surrounding whitespace
    """
    # Ensure all column names are strings
    df.columns = [
        str(col).strip() if col is not None else f"column_{i}"
        for i, col in enumerate(df.columns)
    ]

    # Drop rows and columns that are entirely empty
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)

    # Normalise blank / whitespace-only strings to None
    df = df.replace("", None)
    df = df.replace(r"^\s*$", None, regex=True)

    # Strip leading/trailing whitespace from every string cell
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception as e:
            logger.warning(f"[docx] Could not strip whitespace from column '{col}': {e}")

    return df


# ─────────────────────────────────────────────────────────────
# helper: process one extracted table
# ─────────────────────────────────────────────────────────────

def _process_extracted_table(
    minio_client,
    bucket_name: str,
    object_name: str,
    table_df: pd.DataFrame,
    table_key: str,
    pg_conn,
    catalog_updater,
    uploaded_by: Optional[int],          # ← passed in, not re-fetched per table
):
    """
    Process a single table extracted from a DOCX file:

    0. Normalise and clean the DataFrame  (mirrors pdf_pipeline)
    1. Upload CSV to MinIO
    2. Create (or replace) a PostgreSQL table
    3. Update the data catalog for the CSV

    Fault-tolerant: logs and returns on error so other tables still process.
    """
    try:
        # 0. Normalise — drop empty rows/cols, strip whitespace, blank → None
        table_df = _normalize_dataframe(table_df)

        if table_df.empty or len(table_df.columns) == 0:
            logger.warning(f"[docx] Table empty after normalisation, skipping: {table_key}")
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
        logger.info(f"[docx] Uploaded table CSV → {table_key}")

        # 2. Prepare for PostgreSQL — import helpers from structured_pipeline
        try:
            from pipelines.structured_pipeline import (
                sanitize_table_name,
                sanitize_column_name,
                infer_postgres_type,
                normalize_dataframe as normalize_for_postgres,
            )
        except ImportError as e:
            logger.error(f"[docx] Failed to import from structured_pipeline: {e}")
            logger.warning(f"[docx] Skipping PostgreSQL table creation for {table_key}")
            return

        table_name = sanitize_table_name(table_key)

        # Sanitize column names
        table_df.columns = [sanitize_column_name(col) for col in table_df.columns]

        # Deduplicate column names
        cols = pd.Series(table_df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [
                f"{dup}_{i}" if i != 0 else dup
                for i in range(sum(cols == dup))
            ]
        table_df.columns = cols

        # Final normalisation pass for PostgreSQL (flattens any nested structures)
        table_df = normalize_for_postgres(table_df)

        # 3. Write to PostgreSQL
        cursor = pg_conn.cursor()
        try:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

            column_defs = []
            for col in table_df.columns:
                try:
                    pg_type = infer_postgres_type(table_df[col])
                except Exception as type_err:
                    logger.warning(
                        f"[docx] Could not infer type for column '{col}': {type_err}. Using TEXT."
                    )
                    pg_type = "TEXT"

                # Validate TIMESTAMP/DATE columns: infer_postgres_type uses pandas
                # to_datetime which accepts partial dates like "Dec 2024".
                # PostgreSQL is stricter — "Dec 2024" is NOT a valid timestamp.
                # If validation fails, fall back to TEXT so the COPY doesn't crash.
                if pg_type in ("TIMESTAMP", "DATE"):
                    try:
                        non_null = table_df[col].dropna()
                        if len(non_null) > 0:
                            pd.to_datetime(non_null, format='%Y-%m-%d', exact=False)
                    except Exception:
                        logger.warning(
                            f"[docx] Column '{col}' inferred as {pg_type} but values "
                            f"don't parse as full dates (e.g. 'Dec 2024'). Falling back to TEXT."
                        )
                        pg_type = "TEXT"

                column_defs.append(f'"{col}" {pg_type}')

            cursor.execute(
                f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
            )
            logger.info(f"[docx] Created table: {table_name}")

            # Bulk-load via COPY
            buffer = io.StringIO()
            table_df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)
            cursor.copy_expert(
                f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')',
                buffer,
            )

            pg_conn.commit()
            logger.info(f"[docx] Loaded {len(table_df)} rows into '{table_name}'")

        except Exception as db_err:
            pg_conn.rollback()
            logger.exception(f"[docx] DB operation failed for '{table_name}': {db_err}")
            raise
        finally:
            cursor.close()

        # 4. Update catalog for the extracted table CSV
        try:
            catalog_updater(
                object_name=table_key,
                object_size=len(csv_bytes),
                file_format="csv",
                row_count=len(table_df),
                text_extracted=False,
                uploaded_by=uploaded_by,
            )
        except Exception as catalog_err:
            logger.warning(
                f"[docx] Failed catalog update for table CSV {table_key}: {catalog_err}"
            )

    except Exception as e:
        logger.exception(f"[docx] Failed processing table {table_key}: {e}")
        if pg_conn:
            pg_conn.rollback()
        # Don't re-raise — let the caller process remaining tables


# ─────────────────────────────────────────────────────────────
# main entrypoint
# ─────────────────────────────────────────────────────────────

def process_minio_object(
    minio_client: Minio,
    bucket_name: str,
    object_name: str,
    pg_conn,
    catalog_updater,
    uploaded_by=None,
):
    """
    Main function used by LakehouseETL.run_pipeline_for_object.

    Steps:
    1.  Download DOC/DOCX bytes from MinIO.
    2.  Compute SHA-256 hash & check for duplicates.
    3.  Extract text  (DOCX fully; .doc skipped with a warning).
    4.  Save text to:
          – MinIO : processed/unstructured/text-extracted/<file>.txt
          – Postgres: unstructured_documents table
    5.  Extract tables (DOCX only):
          – Normalise each DataFrame
          – Upload each as CSV → processed/structured/docx-tables/<file>_table_N.csv
          – Create a PostgreSQL table for each
          – Update catalog for each table CSV
    6.  Update minio_data_catalog for the original DOCX/DOC file.
    """

    logger.info(f"[docx] Processing {object_name}")

    # ── Fetch uploader identity from MinIO object metadata once ──────────── #
    stat = minio_client.stat_object(bucket_name, object_name)
    uploaded_by = stat.metadata.get("x-amz-meta-uploaded-by")
    uploaded_by = int(uploaded_by) if uploaded_by else None

    # ── 1. Download bytes ────────────────────────────────────────────────── #
    response = None
    data = None
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = response.read()
    except Exception as e:
        logger.exception(f"[docx] Failed to download {object_name} from MinIO: {e}")
        raise
    finally:
        if response:
            response.close()
            response.release_conn()

    if not data:
        logger.error(f"[docx] No data downloaded for {object_name}")
        return

    file_size = len(data)
    ext = object_name.rsplit(".", 1)[-1].lower()
    root_name = object_name.split("/")[-1].rsplit(".", 1)[0]
    file_type = ext  # 'doc' or 'docx'

    # ── 2. Content extraction ────────────────────────────────────────────── #
    text: str = ""
    tables: List[pd.DataFrame] = []

    if ext == "docx":
        try:
            logger.info("[docx] Extracting text…")
            text = _extract_text_from_docx_bytes(data)
            logger.info(f"[docx] Extracted {len(text)} characters of text")
        except Exception as e:
            logger.exception(f"[docx] Text extraction failed for {object_name}: {e}")

        try:
            logger.info("[docx] Extracting tables…")
            tables = _extract_tables_from_docx_bytes(data)
            logger.info(f"[docx] Found {len(tables)} table(s)")
        except Exception as e:
            logger.exception(f"[docx] Table extraction failed for {object_name}: {e}")

    elif ext == "doc":
        logger.warning(
            "[docx] .doc (binary) format is not supported by python-docx. "
            "Text and tables will not be extracted. "
            "Convert to .docx for full processing."
        )
    else:
        logger.warning(f"[docx] Unexpected extension '{ext}' — treating as binary blob.")

    # ── 4. Save extracted text ───────────────────────────────────────────── #
    if text and text.strip():
        try:
            text_bytes = text.encode("utf-8")
            text_key = f"processed/unstructured/text-extracted/{root_name}.txt"
            minio_client.put_object(
                bucket_name,
                text_key,
                io.BytesIO(text_bytes),
                length=len(text_bytes),
                content_type="text/plain",
            )
            logger.info(f"[docx] Uploaded extracted text → {text_key}")

            # Update catalog for extracted text file
            try:
                catalog_updater(
                    object_name=text_key,
                    object_size=len(text_bytes),
                    file_format="text",
                    row_count=None,
                    text_extracted=True,
                    uploaded_by=uploaded_by,
                    metadata={"source": "docx_text_extraction", "source_docx": object_name}
                )
                logger.info(f"[docx] Created catalog entry for extracted text: {text_key}")
            except Exception as catalog_err:
                logger.warning(f"[docx] Failed to update catalog for extracted text: {catalog_err}")
        except Exception as e:
            logger.exception(f"[docx] Failed to upload extracted text to MinIO: {e}")

        if pg_conn is not None:
            _save_unstructured_doc(
                pg_conn=pg_conn,
                object_name=object_name,
                file_type=file_type,
                text_content=text,
                uploaded_by=uploaded_by,
            )
    else:
        logger.warning(f"[docx] No text extracted from {object_name}")

    # ── 5. Process extracted tables ──────────────────────────────────────── #
    table_count = 0
    total_rows = 0

    if tables:
        logger.info(f"[docx] Processing {len(tables)} table(s)…")
        for idx, df in enumerate(tables, start=1):
            if df.empty:
                logger.debug(f"[docx] Skipping empty table {idx}")
                continue

            table_key = (
                f"processed/structured/docx-tables/{root_name}_table_{idx}.csv"
            )

            _process_extracted_table(
                minio_client=minio_client,
                bucket_name=bucket_name,
                object_name=object_name,
                table_df=df,
                table_key=table_key,
                pg_conn=pg_conn,
                catalog_updater=catalog_updater,
                uploaded_by=uploaded_by,       # pass through — no extra stat call
            )

            table_count += 1
            total_rows += len(df)
            logger.info(
                f"[docx] Processed table {idx}/{len(tables)}: "
                f"{len(df)} rows × {len(df.columns)} cols"
            )

    # ── 6. Catalog update for the original DOCX/DOC file ────────────────────── #
    # The original DOCX remains in the raw-data bucket; processed outputs are:
    #   - extracted text: processed/unstructured/text-extracted/<file>.txt
    #   - extracted tables: processed/structured/docx-tables/<file>_table_N.csv
    try:
        # Collect document-level metadata (only meaningful for .docx)
        doc_metadata: dict = {"table_count": table_count}
        if ext == "docx":
            try:
                doc = Document(io.BytesIO(data))
                props = doc.core_properties
                doc_metadata.update({
                    "author":          props.author,
                    "created":         props.created.isoformat() if props.created else None,
                    "modified":        props.modified.isoformat() if props.modified else None,
                    "title":           props.title,
                    "subject":         props.subject,
                    "category":        props.category,
                    "comments":        props.comments,
                    "paragraph_count": len(doc.paragraphs),
                })
            except Exception as meta_err:
                logger.warning(f"[docx] Could not read core properties: {meta_err}")

        catalog_updater(
            object_name=object_name,
            object_size=file_size,
            file_format=file_type,
            row_count=total_rows,
            text_extracted=True,
            metadata={
                **doc_metadata,
                "status": "processed",
                "extracted_text": f"processed/unstructured/text-extracted/{root_name}.txt",
                "extracted_tables_prefix": f"processed/structured/docx-tables/{root_name}_table_",
            },
            uploaded_by=uploaded_by,
        )
        logger.info(f"[docx] Updated catalog entry for DOCX: {object_name}")

    except Exception as e:
        logger.exception(f"[docx] Failed catalog update for {object_name}: {e}")
        # Don't re-raise — the heavy lifting is already done

    # 7. Remove the raw/ catalog entry (file has been processed)
    if object_name.startswith("raw/"):
        try:
            cursor = pg_conn.cursor()
            cursor.execute(
                "DELETE FROM minio_data_catalog WHERE bucket_name = %s AND object_name = %s",
                (bucket_name, object_name)
            )
            pg_conn.commit()
            cursor.close()
            logger.info(f"[docx] Removed old catalog entry: {object_name}")
        except Exception as delete_err:
            logger.warning(f"[docx] Failed to remove old raw/ entry: {delete_err}")

    logger.info(
        f"[docx] ✅ Completed {object_name} | "
        f"tables={table_count} | total_rows={total_rows} | text_chars={len(text)}"
    )