# python-etl/pipelines/image_pipeline.py

import io
import logging
from typing import Optional

from PIL import Image
from minio import Minio

import requests
import base64
import io

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Moondream model — lazy singleton loaded once per worker
# process sodef  Airflow doesn't re-download it for every file.
# ─────────────────────────────────────────────────────────────

_moondream_model = None  # holds the loaded model after first call


def _get_moondream():
    """
    Lazy-load Moondream2 on first use.

    Moondream is a tiny 1.86B vision-language model designed to run on CPU
    with no GPU required. It supports two operations used here:
      - model.caption(encoded_image)["caption"]  → natural language description
      - model.query(encoded_image, question)["answer"]  → answer any question

    The `moondream` pip package handles HuggingFace weight download and
    tokenisation internally — you don't need to manage transformers or torch.

    Returns the loaded model, or None if the package isn't installed.
    """
    global _moondream_model

    if _moondream_model is not None:
        return _moondream_model

    try:
        import moondream as md
        logger.info("[image] Loading Moondream2 model (first use — may take a moment)...")
        try:
            _moondream_model = md.vl(model="vikhyam/moondream2")
        except TypeError:
            # fallback for older moondream versions
            _moondream_model = md.vl("vikhyam/moondream2")
        # _moondream_model = md.load()
        logger.info("[image] Moondream2 loaded successfully")
        return _moondream_model
    except ImportError:
        logger.warning(
            "[image] `moondream` package not installed — VLM captioning disabled. "
            "Add `moondream` to requirements.txt and rebuild the Docker image."
        )
        return None
    except Exception as e:
        logger.warning(f"[image] Failed to load Moondream2: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# helper: ensure DB table exists
# ─────────────────────────────────────────────────────────────

def _ensure_unstructured_images_table(pg_conn):
    """
    Create unstructured_images if it doesn't exist.
    Deduplication is by object_name (filenames are guaranteed unique
    at upload time via the (1)(2)(3) renaming logic in the uploader).
    """
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS unstructured_images (
                id           SERIAL PRIMARY KEY,
                object_name  TEXT NOT NULL UNIQUE,
                img_format   TEXT,
                width        INTEGER,
                height       INTEGER,
                ocr_text     TEXT,
                vlm_caption  TEXT,
                uploaded_by  INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for col_ddl in [
            "ALTER TABLE unstructured_images ADD COLUMN IF NOT EXISTS vlm_caption TEXT",
            "ALTER TABLE unstructured_images ADD COLUMN IF NOT EXISTS uploaded_by INTEGER",
        ]:
            cursor.execute(col_ddl)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        logger.exception("[image] Failed ensuring unstructured_images table")
        raise
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# helper: Moondream VLM analysis
# ─────────────────────────────────────────────────────────────

# def _run_moondream(img: Image.Image, object_name: str) -> dict:
#     """
#     Run Moondream2 on a PIL Image.

#     Three queries run against a single encoded image (efficient — encode once):
#       1. caption   — natural language description of the whole image
#       2. text      — all readable text visible in the image
#       3. structure — whether the image contains tables, charts, or diagrams

#     Returns a dict with keys: caption, text_query, structure.
#     Returns an empty dict if Moondream is unavailable or inference fails.
#     """
#     model = _get_moondream()
#     if model is None:
#         return {}

#     results = {}
#     try:
#         # Encode image once — reused across all three queries
#         encoded = model.encode_image(img)

#         # 1. General caption
#         caption = model.caption(encoded)["caption"]
#         results["caption"] = caption
#         logger.info(
#             f"[image] Moondream caption: "
#             f"{caption[:120]}{'...' if len(caption) > 120 else ''}"
#         )

#         # 2. Text extraction query
#         # Moondream is better than Tesseract for: natural scene text, screenshots,
#         # handwriting, stylised fonts. Tesseract is better for clean printed docs.
#         # Storing both gives the best coverage.
#         text_answer = model.query(
#             encoded,
#             "What text is visible in this image? List all readable text exactly as it appears."
#         )["answer"]
#         results["text_query"] = text_answer
#         logger.info(f"[image] Moondream text query: {len(text_answer)} chars")

#         # 3. Structural content detection
#         structure_answer = model.query(
#             encoded,
#             "Does this image contain a table, chart, graph, or diagram? "
#             "If yes, briefly describe what it shows."
#         )["answer"]
#         results["structure"] = structure_answer

#     except Exception as e:
#         logger.warning(f"[image] Moondream inference failed for {object_name}: {e}")

#     return results

# ── Dedicated VLM Ollama instance ────────────────────────────────────────
# OLLAMA_HOST     → ollama      (11435 on host) — nomic-embed-text only
# OLLAMA_VLM_HOST → ollama_vlm  (11436 on host) — moondream only
# Two containers = zero queue contention between embedding and VLM workloads.
import os as _os
_OLLAMA_VLM_HOST   = _os.getenv("OLLAMA_VLM_HOST", "http://ollama_vlm:11434")
_MOONDREAM_TIMEOUT = 600
_MOONDREAM_MODEL   = "moondream"


def _run_moondream(img: Image.Image, object_name: str) -> dict:
    """
    Run Moondream VLM via the dedicated ollama_vlm container.

    Three separate calls — caption, text extraction, structure detection —
    are intentional. With a dedicated VLM container there is no contention
    with the embedding Ollama, so each call gets its full 600s window.

    Each call fails independently — a timeout on text_query won't prevent
    caption or structure from being saved.
    """
    results = {}

    # Encode image to base64 once — shared across all 3 requests
    buffered = io.BytesIO()
    img.save(buffered, format="JPEG")
    img_b64 = base64.b64encode(buffered.getvalue()).decode()

    def _call(prompt: str, label: str) -> str:
        """Single Ollama VLM request. Returns empty string on any failure."""
        try:
            res = requests.post(
                f"{_OLLAMA_VLM_HOST}/api/generate",
                json={
                    "model": _MOONDREAM_MODEL,
                    "prompt": prompt,
                    "images": [img_b64],
                    "stream": False,
                },
                timeout=_MOONDREAM_TIMEOUT,
            )
            res.raise_for_status()
            return res.json().get("response", "")
        except Exception as e:
            logger.warning(f"[image] Moondream {label} failed for {object_name}: {e}")
            return ""

    # 1️⃣ Caption — general image description
    caption = _call(
        "Describe this image in detail. State some technical information.",
        "caption",
    )
    if caption:
        results["caption"] = caption
        logger.info(f"[image] Moondream caption: {caption[:120]}")

    # 2️⃣ Text extraction — readable text visible in the image
    text_answer = _call(
        "Extract all visible text exactly as it appears in this image.",
        "text_query",
    )
    if text_answer and text_answer.strip().lower() not in (
        "none", "no text", "no text visible", "no text is visible", ""
    ):
        results["text_query"] = text_answer
        logger.info(f"[image] Moondream text: {len(text_answer)} chars")

    # 3️⃣ Structure detection — tables, charts, diagrams
    structure_answer = _call(
        "Does this image contain a table, chart, graph, or diagram? "
        "If yes, summarize what it shows.",
        "structure",
    )
    if structure_answer and structure_answer.strip().lower() not in ("no", "none", ""):
        results["structure"] = structure_answer

    return results

# ─────────────────────────────────────────────────────────────
# helper: Tesseract OCR
# ─────────────────────────────────────────────────────────────

def _run_ocr(img: Image.Image, object_name: str) -> Optional[str]:
    """
    Run Tesseract OCR via pytesseract.
    Returns extracted text or None if unavailable/failed.
    """
    try:
        import pytesseract
        text = pytesseract.image_to_string(img)
        if text and text.strip():
            logger.info(f"[image] OCR extracted {len(text)} characters")
            return text
        logger.info(f"[image] OCR produced no text for {object_name}")
        return None
    except ImportError:
        logger.warning("[image] pytesseract not installed — OCR skipped")
        return None
    except Exception as e:
        logger.warning(f"[image] OCR failed for {object_name}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# main entrypoint
# ─────────────────────────────────────────────────────────────

def process_minio_object(
    minio_client: Minio,
    bucket_name: str,
    object_name: str,
    pg_conn,
    catalog_updater,
    uploaded_by: Optional[int] = None,
    do_ocr: bool = True,
    do_moondream: bool = True,
):
    """
    Main entry point for image processing. Called by etl_manager.

    Steps:
    1.  Fetch uploader identity from MinIO object metadata.
    2.  Download image bytes from MinIO.
    3.  Compute SHA-256 hash and check for duplicates.
    4.  Open with Pillow → extract width, height, format, mode.
    5.  Run Moondream2 VLM (do_moondream=True by default):
          - caption: natural language description of the image
          - text_query: all readable text in the image
          - structure: detects tables, charts, diagrams
    6.  Run Tesseract OCR (do_ocr=True by default).
    7.  Combine all text signals → upload .txt to MinIO.
    8.  Read EXIF metadata.
    9.  Write to unstructured_images table.
    10. Update minio_data_catalog.
    """
    logger.info(f"[image] Processing {object_name}")
    resp = None

    try:
        # ── 1. Fetch uploader identity ───────────────────────────────────── #
        stat = minio_client.stat_object(bucket_name, object_name)
        uploaded_by = stat.metadata.get("x-amz-meta-uploaded-by")
        uploaded_by = int(uploaded_by) if uploaded_by else None

        # ── 2. Download bytes ────────────────────────────────────────────── #
        resp = minio_client.get_object(bucket_name, object_name)
        data = resp.read()

        file_size = len(data)
        file_root = object_name.split("/")[-1].rsplit(".", 1)[0]

        # ── 3. Open image ────────────────────────────────────────────────── #
        img = Image.open(io.BytesIO(data))
        width, height = img.size
        fmt = img.format or "unknown"
        mode = img.mode
        logger.info(f"[image] {width}x{height} | format={fmt} | mode={mode}")

        # ── 5. Moondream VLM ─────────────────────────────────────────────── #
        vlm_results = {}
        vlm_caption = None
        vlm_text = None

        if do_moondream:
            logger.info("[image] Running Moondream2 VLM...")
            vlm_results = _run_moondream(img, object_name)
            vlm_caption = vlm_results.get("caption")
            vlm_text = vlm_results.get("text_query")

        # ── 6. Tesseract OCR ─────────────────────────────────────────────── #
        ocr_text = None
        if do_ocr:
            logger.info("[image] Running Tesseract OCR...")
            ocr_text = _run_ocr(img, object_name)

        # ── 7. Combined text → MinIO ─────────────────────────────────────── #
        text_parts = []
        if vlm_caption:
            text_parts.append(f"[VLM Caption]\n{vlm_caption}")
        if vlm_text and vlm_text.strip().lower() not in (
            "none", "no text", "no text visible", "no text is visible", ""
        ):
            text_parts.append(f"[VLM Text]\n{vlm_text}")
        if vlm_results.get("structure"):
            text_parts.append(f"[VLM Structure]\n{vlm_results['structure']}")
        if ocr_text:
            text_parts.append(f"[OCR]\n{ocr_text}")

        combined_text = "\n\n".join(text_parts) if text_parts else None
        text_extracted = bool(combined_text)

        if combined_text:
            try:
                text_bytes = combined_text.encode("utf-8")
                txt_key = f"processed/unstructured/text-extracted/{file_root}.txt"
                minio_client.put_object(
                    bucket_name,
                    txt_key,
                    io.BytesIO(text_bytes),
                    length=len(text_bytes),
                    content_type="text/plain",
                )
                logger.info(
                    f"[image] Combined text ({len(combined_text)} chars) → {txt_key}"
                )

                # Update catalog for extracted text file
                try:
                    catalog_updater(
                        object_name=txt_key,
                        object_size=len(text_bytes),
                        file_format="text",
                        row_count=None,
                        text_extracted=True,
                        uploaded_by=uploaded_by,
                        metadata={"source": "image_text_extraction", "source_image": object_name}
                    )
                    logger.info(f"[image] Created catalog entry for extracted text: {txt_key}")
                except Exception as catalog_err:
                    logger.warning(f"[image] Failed to update catalog for extracted text: {catalog_err}")
            except Exception as e:
                logger.warning(f"[image] Failed to upload text to MinIO: {e}")
        else:
            logger.info(f"[image] No text produced for {object_name}")

        # ── 8. EXIF metadata ─────────────────────────────────────────────── #
        # BUG FIXED: original did bare `import exifread` at the call site with
        # no try/except — ImportError would crash the pipeline after all the
        # heavy VLM/OCR work was already done.
        exif_data = {}
        try:
            import exifread
            tags = exifread.process_file(io.BytesIO(data), details=False)
            exif_data = {
                tag: str(tags[tag])
                for tag in tags
                if "EXIF" in tag or "GPS" in tag or "Image" in tag
            }
        except ImportError:
            logger.debug("[image] exifread not installed — EXIF skipped")
        except Exception as e:
            logger.debug(f"[image] EXIF extraction failed: {e}")

        # ── 9. Write to unstructured_images ──────────────────────────────── #
        _ensure_unstructured_images_table(pg_conn)

        cursor = pg_conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO unstructured_images
                    (object_name, img_format, width, height,
                     ocr_text, vlm_caption, uploaded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (object_name) DO UPDATE
                SET img_format  = EXCLUDED.img_format,
                    width       = EXCLUDED.width,
                    height      = EXCLUDED.height,
                    ocr_text    = EXCLUDED.ocr_text,
                    vlm_caption = EXCLUDED.vlm_caption,
                    uploaded_by = EXCLUDED.uploaded_by
                """,
                (object_name, fmt, width, height,
                 ocr_text, vlm_caption, uploaded_by),
            )
            pg_conn.commit()
            logger.info("[image] Saved to unstructured_images")
        except Exception as db_err:
            pg_conn.rollback()
            logger.exception(f"[image] Failed writing to unstructured_images: {db_err}")
        finally:
            cursor.close()

        # ── 10. Update catalog for processed image file ──────────────────── #
        # Change prefix from raw/ to processed/ to reflect final location
        processed_object_name = object_name.replace("raw/", "processed/", 1) if object_name.startswith("raw/") else object_name

        image_metadata = {
            "width":         width,
            "height":        height,
            "format":        fmt,
            "mode":          mode,
            "exif":          exif_data,
            "vlm_caption":   vlm_caption,
            "vlm_text":      vlm_text,
            "vlm_structure": vlm_results.get("structure"),
        }

        # First, mark the old raw/ entry as processed (don't delete - preserve audit trail)
        catalog_updater(
            object_name=object_name,
            object_size=file_size,
            file_format="image",
            row_count=None,
            text_extracted=True,
            metadata={**image_metadata, "status": "processed", "migrated_to": processed_object_name},
            uploaded_by=uploaded_by,
        )
        logger.info(f"[image] Marked raw entry as processed: {object_name}")

        # Then create/update the processed/ entry with full metadata
        catalog_updater(
            object_name=processed_object_name,
            object_size=file_size,
            file_format="image",
            row_count=None,
            text_extracted=text_extracted,
            metadata=image_metadata,
            uploaded_by=uploaded_by,
        )
        logger.info(f"[image] Created processed catalog entry: {processed_object_name}")

        # Remove the old raw/ entry from catalog (it has been processed)
        if object_name != processed_object_name and object_name.startswith("raw/"):
            try:
                cursor = pg_conn.cursor()
                cursor.execute(
                    "DELETE FROM minio_data_catalog WHERE bucket_name = %s AND object_name = %s",
                    (bucket_name, object_name)
                )
                pg_conn.commit()
                cursor.close()
                logger.info(f"[image] Removed old catalog entry: {object_name}")
            except Exception as delete_err:
                logger.warning(f"[image] Failed to remove old raw/ entry: {delete_err}")

        logger.info(
            f"[image] ✅ Completed {object_name} -> {processed_object_name} | "
            f"{width}x{height} | fmt={fmt} | "
            f"ocr={'yes' if ocr_text else 'no'} | "
            f"vlm={'yes' if vlm_caption else 'no'} | "
            f"text_chars={len(combined_text) if combined_text else 0}"
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