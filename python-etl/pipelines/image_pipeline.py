import io
import logging
import hashlib
from PIL import Image
import pytesseract
import os

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
    finally:
        cursor.close()


def _ensure_unstructured_images_table(pg_conn):
    """
    Make sure unstructured_images table exists with all required columns.
    """
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS unstructured_images (
                id SERIAL PRIMARY KEY,
                object_name TEXT NOT NULL,
                img_format TEXT,
                width INTEGER,
                height INTEGER,
                ocr_text TEXT,
                content_hash TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        logger.exception("[image] Failed ensuring unstructured_images table")
        raise
    finally:
        cursor.close()


def process_minio_object(minio_client, bucket_name, object_name, pg_conn, catalog_updater, do_ocr=True):
    """
    Process image files from MinIO.
    
    Steps:
    1. Downloads image from MinIO
    2. Computes SHA256 hash and checks for duplicates
    3. Extracts basic metadata (width, height, format)
    4. Optionally runs OCR (requires pytesseract installed and Tesseract binary available)
    5. Saves metadata to Postgres table unstructured_images
    6. Optionally saves OCR text to:
       - MinIO: processed/unstructured/text-extracted/<file>.txt
       - Postgres: unstructured_images.ocr_text column
    7. Updates catalog
    """
    logger.info(f"[image] Processing {object_name}")
    resp = None
    
    try:
        # 1. Download from MinIO
        resp = minio_client.get_object(bucket_name, object_name)
        data = resp.read()
        
        file_size = len(data)
        file_hash = calculate_file_hash(data)
        file_root = object_name.split("/")[-1].rsplit(".", 1)[0]
        
        # 2. Check for duplicates
        if is_duplicate(pg_conn, file_hash):
            logger.info(
                f"[image] Duplicate detected via content_hash, "
                f"skipping heavy processing: {object_name}"
            )
            # Still update catalog with basic info
            try:
                catalog_updater(
                    object_name=object_name,
                    object_size=file_size,
                    file_format="image",
                    row_count=0,
                    text_extracted=False,
                    content_hash=file_hash,
                )
            except Exception:
                logger.exception("[image] Failed catalog update for duplicate")
            return
        
        # 3. Open image and extract metadata
        img = Image.open(io.BytesIO(data))
        width, height = img.size
        fmt = img.format or "unknown"
        
        logger.info(f"[image] Image metadata: {width}x{height}, format={fmt}")

        # 4. Run OCR if enabled
        ocr_text = None
        if do_ocr:
            try:
                logger.info(f"[image] Running OCR on {object_name}...")
                ocr_text = pytesseract.image_to_string(img)
                
                if ocr_text and ocr_text.strip():
                    logger.info(f"[image] OCR extracted {len(ocr_text)} characters")
                    
                    # Save OCR text to MinIO
                    txt_name = f"processed/unstructured/text-extracted/{file_root}.txt"
                    text_bytes = ocr_text.encode('utf-8')
                    
                    minio_client.put_object(
                        bucket_name, 
                        txt_name, 
                        io.BytesIO(text_bytes),
                        length=len(text_bytes),
                        content_type='text/plain'
                    )
                    logger.info(f"[image] OCR text uploaded to {txt_name}")
                else:
                    logger.info(f"[image] OCR produced no text for {object_name}")
                    
            except Exception as e:
                logger.warning(f"[image] OCR failed for {object_name}: {e}")
                ocr_text = None

        # 5. Save metadata to Postgres
        _ensure_unstructured_images_table(pg_conn)
        
        cursor = pg_conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO unstructured_images 
                    (object_name, img_format, width, height, ocr_text, content_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_hash) DO UPDATE
                SET object_name = EXCLUDED.object_name,
                    img_format = EXCLUDED.img_format,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    ocr_text = EXCLUDED.ocr_text
                """,
                (object_name, fmt, width, height, ocr_text, file_hash)
            )
            pg_conn.commit()
            logger.info(f"[image] Saved metadata to unstructured_images for {object_name}")
        finally:
            cursor.close()

        # 6. Update catalog
        import exifread
        tags = exifread.process_file(io.BytesIO(data)) if data else {}
        exif_data = {tag: str(tags[tag]) for tag in tags if 'EXIF' in tag or 'GPS' in tag}  # e.g., {'EXIF DateTimeOriginal': '2025:01:01 12:00:00'}

        image_metadata = {
            'width': width,
            'height': height,
            'format': fmt,
            'mode': img.mode if img else None,
            'exif': exif_data
        }

        # Update catalog with metadata
        catalog_updater(
            object_name=object_name,
            object_size=file_size,
            file_format='image',
            row_count=None,
            text_extracted=bool(ocr_text and ocr_text.strip()),
            content_hash=file_hash,
            metadata=image_metadata  # NEW
        )
        
        logger.info(
            f"[image] ✅ Completed processing {object_name} | "
            f"size={width}x{height} | format={fmt} | ocr_chars={len(ocr_text) if ocr_text else 0}"
        )
        
    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        logger.exception(f"[image] Error processing {object_name}: {e}")
        raise
        
    finally:
        if resp:
            resp.close()
            resp.release_conn()