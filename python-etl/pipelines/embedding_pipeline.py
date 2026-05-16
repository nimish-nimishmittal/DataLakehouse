# python-etl/pipelines/embedding_pipeline.py
"""
Embedding pipeline — dual-vector support.

Two embedding backends, two separate DB columns:

  embedding_768  vector(768)  ← nomic-embed-text via Ollama (primary)
  embedding_384  vector(384)  ← all-MiniLM-L6-v2 via sentence-transformers (fallback)

Both columns can be populated on the same row.  Search prefers
embedding_768 when available, falls back to embedding_384.

When Ollama is down:
  - MiniLM runs and fills embedding_384
  - embedding_768 stays NULL for that row
  - Next DAG run with Ollama healthy re-embeds those rows into embedding_768

When Ollama is healthy:
  - Ollama fills embedding_768
  - MiniLM also runs and fills embedding_384 (optional, controlled by EMBED_BOTH)
  - Default: EMBED_BOTH=false — only embed with whatever model succeeds first
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from typing import Optional

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
OLLAMA_HOST       = os.getenv("OLLAMA_HOST",        "http://ollama:11434")
OLLAMA_MODEL      = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
OLLAMA_TIMEOUT    = int(os.getenv("OLLAMA_TIMEOUT",    "300"))  # seconds
OLLAMA_BATCH_SIZE = int(os.getenv("OLLAMA_BATCH_SIZE", "32"))   # texts per request

# If True: always fill BOTH columns (slower, more thorough).
# If False (default): fill 768 via Ollama; only use MiniLM if Ollama fails.
EMBED_BOTH = os.getenv("EMBED_BOTH", "false").lower() == "true"


# ── Ollama backend (768-dim) ─────────────────────────────────

def _embed_ollama(texts: list[str]) -> list[list[float]] | None:
    """
    Call Ollama /api/embed in small batches.
    Returns 768-dim vectors or None if Ollama is unreachable/fails.
    Timeout is 300 s to handle cold model load on first request.
    """
    import urllib.request, urllib.error

    url = f"{OLLAMA_HOST}/api/embed"
    all_vectors = []

    for i in range(0, len(texts), OLLAMA_BATCH_SIZE):
        chunk = texts[i : i + OLLAMA_BATCH_SIZE]
        payload = json.dumps({"model": OLLAMA_MODEL, "input": chunk}).encode()
        try:
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp:
                result = json.loads(resp.read())
                vecs = result.get("embeddings") or result.get("embedding")
                if not vecs:
                    logger.warning(f"[embed] Ollama returned empty for chunk {i}")
                    return None
                all_vectors.extend(vecs)
        except urllib.error.URLError as e:
            logger.warning(f"[embed] Ollama unreachable: {e}")
            return None
        except Exception as e:
            logger.warning(f"[embed] Ollama call failed: {e}")
            return None

    return all_vectors


# ── MiniLM backend (384-dim) ─────────────────────────────────

_st_model = None

def _embed_minilm(texts: list[str]) -> list[list[float]] | None:
    """
    sentence-transformers all-MiniLM-L6-v2 — 384-dim, CPU-only.
    Loads once per process and stays in memory.
    Always produces 384-dim vectors regardless of OLLAMA config.
    """
    global _st_model
    try:
        if _st_model is None:
            from sentence_transformers import SentenceTransformer
            logger.info("[embed] Loading all-MiniLM-L6-v2...")
            _st_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("[embed] MiniLM loaded.")
        vecs = _st_model.encode(texts, batch_size=32, show_progress_bar=False)
        return [v.tolist() for v in vecs]
    except ImportError:
        logger.error("[embed] sentence-transformers not installed.")
        return None
    except Exception as e:
        logger.error(f"[embed] MiniLM failed: {e}")
        return None


# ── Row serialisation ─────────────────────────────────────────

def _row_to_text(table_name: str, row_data: dict) -> str:
    """
    Serialise a row into human-readable text for embedding.
    Format: "table: data doc1 sales | region: North | sales: 45000 | ..."
    NULLs skipped, values truncated at 500 chars.
    """
    readable = table_name.replace("_", " ").strip()
    parts = [f"table: {readable}"]
    for col, val in row_data.items():
        if val is None:
            continue
        s = str(val)
        parts.append(f"{col}: {s[:500]}{'…' if len(s) > 500 else ''}")
    return " | ".join(parts)


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "lakehouse_db"),
        user=os.getenv("POSTGRES_USER", "lakehouse_user"),
        password=os.getenv("POSTGRES_PASSWORD", "lakehouse_pass"),
    )


# ── Trigger attachment ────────────────────────────────────────

def attach_triggers_for_new_tables(pg_conn) -> int:
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE 'data_%'
              AND tablename NOT IN (
                  SELECT event_object_table FROM information_schema.triggers
                  WHERE trigger_name LIKE 'trg_embed_%'
              )
            """
        )
        new_tables = [row[0] for row in cursor.fetchall()]
        for tbl in new_tables:
            cursor.execute("SELECT attach_embedding_trigger(%s)", (tbl,))
            logger.info(f"[embed] Trigger attached: {tbl}")
        if new_tables:
            pg_conn.commit()
        return len(new_tables)
    except Exception as e:
        pg_conn.rollback()
        logger.warning(f"[embed] Trigger attach failed: {e}")
        return 0
    finally:
        cursor.close()


# ── Cleanup: dropped tables ──────────────────────────────────

def cleanup_orphaned_embeddings(pg_conn) -> dict:
    """
    Remove embeddings from row_embeddings whose source table no longer
    exists in pg_tables (i.e. the table was DROPped).

    This closes the gap that DROP TABLE creates:
      - DROP TABLE removes the trigger with the table, so no DELETE events
        get queued.  row_embeddings would keep stale vectors forever.
      - This function detects those ghost table names and purges them in
        one DELETE per orphaned table.

    Returns:
        {"orphaned_tables": [...], "removed_embeddings": N}
    """
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    total_removed = 0
    orphaned_tables = []

    try:
        # All distinct table names that have embeddings
        cursor.execute(
            "SELECT DISTINCT table_name FROM row_embeddings ORDER BY table_name"
        )
        embedded_tables = {r["table_name"] for r in cursor.fetchall()}

        if not embedded_tables:
            return {"orphaned_tables": [], "removed_embeddings": 0}

        # All tables that actually exist right now
        cursor.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
            """
        )
        existing_tables = {r["tablename"] for r in cursor.fetchall()}

        orphaned = embedded_tables - existing_tables

        if not orphaned:
            logger.info("[cleanup] No orphaned embedding tables found.")
            return {"orphaned_tables": [], "removed_embeddings": 0}

        logger.warning(
            f"[cleanup] Found {len(orphaned)} orphaned table(s) in row_embeddings: "
            f"{sorted(orphaned)}"
        )

        # Delete all embeddings for each orphaned table
        dc = pg_conn.cursor()
        try:
            for tbl in sorted(orphaned):
                dc.execute(
                    "DELETE FROM row_embeddings WHERE table_name = %s",
                    (tbl,),
                )
                n = dc.rowcount
                total_removed += n
                orphaned_tables.append(tbl)
                logger.info(
                    f"[cleanup] Removed {n} orphaned embeddings for dropped table: {tbl}"
                )

                # Also clean up any pending queue events for this table
                dc.execute(
                    "DELETE FROM embedding_queue WHERE table_name = %s AND status = 'pending'",
                    (tbl,),
                )
                logger.info(f"[cleanup] Cleared pending queue entries for: {tbl}")

            pg_conn.commit()
            logger.info(
                f"[cleanup] Orphan cleanup done — "
                f"{len(orphaned_tables)} table(s), {total_removed} embeddings removed."
            )
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"[cleanup] Orphan delete failed: {e}")
            raise
        finally:
            dc.close()

        return {"orphaned_tables": orphaned_tables, "removed_embeddings": total_removed}

    except Exception as e:
        pg_conn.rollback()
        logger.exception(f"[cleanup] cleanup_orphaned_embeddings failed: {e}")
        raise
    finally:
        cursor.close()


def cleanup_truncated_tables(pg_conn) -> dict:
    """
    Detect tables that were TRUNCATE'd and remove stale embeddings.

    TRUNCATE TABLE bypasses row-level triggers entirely, so no DELETE
    events reach embedding_queue.  This function detects the mismatch:

      source_table has 0 rows  →  remove ALL embeddings for that table
      source_table has N rows  →  remove embeddings for row_ids that no
                                   longer exist in the source table

    Only runs on tables that still exist (orphan cleanup handles dropped ones).
    Per-table row-existence check is done with a NOT EXISTS join — fast when
    the source table has an index on id, which all data_* tables do.

    Returns:
        {"checked_tables": N, "removed_embeddings": N}
    """
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    total_removed = 0
    checked = 0

    try:
        # Find tables that both exist AND have embeddings
        cursor.execute(
            """
            SELECT DISTINCT re.table_name
            FROM row_embeddings re
            JOIN pg_tables pt
              ON pt.tablename = re.table_name
             AND pt.schemaname = 'public'
            ORDER BY re.table_name
            """
        )
        live_embedded_tables = [r["table_name"] for r in cursor.fetchall()]

        if not live_embedded_tables:
            return {"checked_tables": 0, "removed_embeddings": 0}

        dc = pg_conn.cursor()
        try:
            for tbl in live_embedded_tables:
                # Check if the source table has an 'id' column
                cursor.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                      AND column_name = 'id' LIMIT 1
                    """,
                    (tbl,),
                )
                has_id = cursor.fetchone() is not None

                if not has_id:
                    # No id column — can't do row-level reconciliation safely
                    # Just check if the table is completely empty
                    dc.execute(f'SELECT 1 FROM "{tbl}" LIMIT 1')
                    if dc.fetchone() is None:
                        dc.execute(
                            "DELETE FROM row_embeddings WHERE table_name = %s",
                            (tbl,),
                        )
                        n = dc.rowcount
                        if n:
                            total_removed += n
                            logger.info(
                                f"[cleanup] TRUNCATE detected (no id col): "
                                f"removed {n} embeddings from {tbl}"
                            )
                    checked += 1
                    continue

                # Delete embeddings whose row_id no longer exists in the source
                dc.execute(
                    f"""
                    DELETE FROM row_embeddings re
                    WHERE re.table_name = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM "{tbl}" src
                          WHERE src.id::TEXT = re.row_id
                      )
                    """,
                    (tbl,),
                )
                n = dc.rowcount
                if n > 0:
                    total_removed += n
                    logger.info(
                        f"[cleanup] Removed {n} stale embeddings from {tbl} "
                        f"(TRUNCATE or bulk DELETE detected)"
                    )
                checked += 1

            pg_conn.commit()
            logger.info(
                f"[cleanup] Truncate/bulk-delete check done — "
                f"checked {checked} tables, removed {total_removed} stale embeddings."
            )
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"[cleanup] Truncate cleanup failed: {e}")
            raise
        finally:
            dc.close()

        return {"checked_tables": checked, "removed_embeddings": total_removed}

    except Exception as e:
        pg_conn.rollback()
        logger.exception(f"[cleanup] cleanup_truncated_tables failed: {e}")
        raise
    finally:
        cursor.close()


# ── Backfill ─────────────────────────────────────────────────

def backfill_all_tables(pg_conn, skip_already_embedded: bool = True) -> int:
    """
    Queue existing rows that don't yet have embeddings.

    Smart completion detection — once all rows are fully embedded
    (both embedding_768 AND embedding_384 populated), this function
    costs only two fast COUNT queries and exits immediately.
    No per-table scanning, no impact on querying performance.

    Rows with only one column filled (e.g. MiniLM-only from an Ollama
    outage) are still re-queued so the missing column gets filled.
    """
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    total_queued = 0

    try:
        # ── Fast completion check ─────────────────────────────────────
        # Use pg_class planner stats for a cheap row estimate —
        # no full table scans needed for this check.
        cursor.execute(
            """
            SELECT COALESCE(SUM(reltuples::bigint), 0) AS total_source_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND (c.relname LIKE 'data_%'
                   OR c.relname IN ('unstructured_documents',
                                    'unstructured_images',
                                    'minio_data_catalog'))
            """
        )
        total_source = cursor.fetchone()["total_source_rows"]

        cursor.execute(
            """
            SELECT COUNT(*) AS fully_embedded
            FROM row_embeddings
            WHERE embedding_768 IS NOT NULL
              AND embedding_384 IS NOT NULL
            """
        )
        fully_embedded = cursor.fetchone()["fully_embedded"]

        if total_source > 0 and fully_embedded >= total_source:
            logger.info(
                f"[embed] Backfill complete — all ~{total_source} rows fully embedded. "
                f"Skipping scan."
            )
            return 0

        logger.info(
            f"[embed] Backfill needed — source~{total_source}, "
            f"fully embedded={fully_embedded}, gap~{total_source - fully_embedded}"
        )

        # ── Per-table scan (only when backfill still in progress) ─────
        cursor.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
              AND (tablename LIKE 'data_%'
                   OR tablename IN ('unstructured_documents',
                                    'unstructured_images',
                                    'minio_data_catalog'))
            ORDER BY tablename
            """
        )
        tables = [r["tablename"] for r in cursor.fetchall()]
        logger.info(f"[embed] Scanning {len(tables)} tables for unembedded rows...")

        for tbl in tables:
            try:
                cursor.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                      AND column_name = 'id' LIMIT 1
                    """,
                    (tbl,),
                )
                pk_expr = "id::TEXT" if cursor.fetchone() else "ctid::TEXT"

                if skip_already_embedded:
                    # Only queue rows missing at least one embedding column
                    cursor.execute(
                        f"""
                        INSERT INTO embedding_queue
                            (table_name, row_id, operation, row_data)
                        SELECT %s, {pk_expr}, 'INSERT', to_jsonb(t)
                        FROM "{tbl}" t
                        WHERE {pk_expr} NOT IN (
                            SELECT row_id FROM row_embeddings
                            WHERE table_name = %s
                              AND embedding_768 IS NOT NULL
                              AND embedding_384 IS NOT NULL
                        )
                        """,
                        (tbl, tbl),
                    )
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO embedding_queue
                            (table_name, row_id, operation, row_data)
                        SELECT %s, {pk_expr}, 'INSERT', to_jsonb(t)
                        FROM "{tbl}" t
                        """,
                        (tbl,),
                    )

                n = cursor.rowcount
                if n > 0:
                    logger.info(f"[embed] Queued {n} rows from {tbl}")
                    total_queued += n

            except Exception as e:
                pg_conn.rollback()
                logger.warning(f"[embed] Backfill failed for {tbl}: {e}")
                continue

        pg_conn.commit()
        logger.info(f"[embed] Backfill complete — {total_queued} rows queued")
        return total_queued

    except Exception as e:
        pg_conn.rollback()
        raise
    finally:
        cursor.close()


# ── Main queue processing ─────────────────────────────────────

def process_embedding_queue(batch_size: int = 100, max_batches: int = 50) -> dict:
    """
    Read pending queue events, generate embeddings, upsert into row_embeddings.

    For each batch:
      - Try Ollama → fills embedding_768
      - If EMBED_BOTH=true OR Ollama failed → try MiniLM → fills embedding_384
      - Upsert uses ON CONFLICT so both columns can be filled across separate runs
    """
    pg_conn = _get_pg_conn()
    pg_conn.autocommit = False

    total_processed = 0
    total_errors = 0

    try:
        n = attach_triggers_for_new_tables(pg_conn)
        if n:
            logger.info(f"[embed] Attached triggers to {n} new table(s)")

        for batch_num in range(max_batches):
            cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute(
                    """
                    SELECT id, table_name, row_id, operation, row_data
                    FROM embedding_queue
                    WHERE status = 'pending'
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (batch_size,),
                )
                events = cur.fetchall()
            finally:
                cur.close()

            if not events:
                logger.info(f"[embed] Queue empty after {total_processed} rows.")
                break

            logger.info(f"[embed] Batch {batch_num + 1}: {len(events)} events")

            queue_ids_done  = []
            queue_ids_error = {}
            to_embed  = []
            to_delete = []

            for ev in events:
                if ev["operation"] == "DELETE":
                    to_delete.append((ev["id"], ev["table_name"], ev["row_id"]))
                elif ev["row_data"]:
                    to_embed.append((
                        ev["id"], ev["table_name"], ev["row_id"], dict(ev["row_data"])
                    ))
                else:
                    queue_ids_error[ev["id"]] = "Missing row_data"

            # ── DELETEs ──────────────────────────────────────
            if to_delete:
                dc = pg_conn.cursor()
                try:
                    for qid, tbl, rid in to_delete:
                        dc.execute(
                            "DELETE FROM row_embeddings WHERE table_name=%s AND row_id=%s",
                            (tbl, rid),
                        )
                        queue_ids_done.append(qid)
                    pg_conn.commit()
                except Exception as e:
                    pg_conn.rollback()
                    for qid, _, _ in to_delete:
                        queue_ids_error[qid] = str(e)
                finally:
                    dc.close()

            # ── INSERT / UPDATE ───────────────────────────────
            if to_embed:
                texts = [_row_to_text(tbl, data) for _, tbl, _, data in to_embed]

                # ── Try Ollama (768-dim) ──────────────────────
                vecs_768 = _embed_ollama(texts)
                if vecs_768:
                    logger.info(f"[embed] Ollama produced {len(vecs_768)} × 768-dim vectors")
                else:
                    logger.warning("[embed] Ollama unavailable — embedding_768 will be NULL for this batch")

                # ── Try MiniLM (384-dim) ──────────────────────
                # Always run if: Ollama failed, OR EMBED_BOTH=true
                vecs_384 = None
                if not vecs_768 or EMBED_BOTH:
                    vecs_384 = _embed_minilm(texts)
                    if vecs_384:
                        logger.info(f"[embed] MiniLM produced {len(vecs_384)} × 384-dim vectors")
                    else:
                        logger.error("[embed] MiniLM also failed — batch will error")

                # If both failed, mark all as error
                if not vecs_768 and not vecs_384:
                    for qid, _, _, _ in to_embed:
                        queue_ids_error[qid] = "Both Ollama and MiniLM failed"
                else:
                    # ── Upsert into row_embeddings ────────────
                    uc = pg_conn.cursor()
                    try:
                        for i, (qid, tbl, rid, row_data) in enumerate(to_embed):
                            v768 = str(vecs_768[i]) if vecs_768 else None
                            v384 = str(vecs_384[i]) if vecs_384 else None
                            text = texts[i]

                            # Determine embed_source label
                            if vecs_768 and vecs_384:
                                source = "both"
                            elif vecs_768:
                                source = "ollama_768"
                            else:
                                source = "minilm_384"

                            uc.execute(
                                """
                                INSERT INTO row_embeddings
                                    (table_name, row_id, row_text,
                                     embedding_768, embedding_384,
                                     embed_source, row_data)
                                VALUES (%s, %s, %s,
                                        %s::vector, %s::vector,
                                        %s, %s)
                                ON CONFLICT (table_name, row_id) DO UPDATE
                                SET row_text      = EXCLUDED.row_text,
                                    embedding_768 = COALESCE(EXCLUDED.embedding_768,
                                                             row_embeddings.embedding_768),
                                    embedding_384 = COALESCE(EXCLUDED.embedding_384,
                                                             row_embeddings.embedding_384),
                                    embed_source  = EXCLUDED.embed_source,
                                    row_data      = EXCLUDED.row_data,
                                    updated_at    = CURRENT_TIMESTAMP
                                """,
                                (tbl, rid, text,
                                 v768, v384,
                                 source, json.dumps(row_data)),
                            )
                            queue_ids_done.append(qid)

                        pg_conn.commit()
                        logger.info(f"[embed] Upserted {len(to_embed)} rows")

                    except Exception as db_err:
                        pg_conn.rollback()
                        logger.error(f"[embed] DB upsert failed: {db_err}")
                        for qid, _, _, _ in to_embed:
                            queue_ids_error[qid] = str(db_err)
                    finally:
                        uc.close()

            # ── Mark queue entries ────────────────────────────
            mc = pg_conn.cursor()
            try:
                if queue_ids_done:
                    mc.execute(
                        "UPDATE embedding_queue SET status='done', "
                        "processed_at=CURRENT_TIMESTAMP WHERE id=ANY(%s)",
                        (queue_ids_done,),
                    )
                for qid, msg in queue_ids_error.items():
                    mc.execute(
                        "UPDATE embedding_queue SET status='error', error_msg=%s, "
                        "processed_at=CURRENT_TIMESTAMP WHERE id=%s",
                        (msg, qid),
                    )
                pg_conn.commit()
            finally:
                mc.close()

            total_processed += len(queue_ids_done)
            total_errors    += len(queue_ids_error)

        logger.info(f"[embed] ✅ Done. processed={total_processed} errors={total_errors}")
        return {"processed": total_processed, "errors": total_errors}

    except Exception as e:
        logger.exception(f"[embed] Fatal: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


# ── Search ────────────────────────────────────────────────────

def search(
    query: str,
    top_k: int = 10,
    table_filter: "str | list[str] | None" = None,
    pg_conn=None,
) -> list[dict]:
    """
    Semantic search across all embedded rows.

    Args:
        query:        Natural language question or phrase.
        top_k:        Number of results (default 10).
        table_filter: None          → search all tables
                      str           → restrict to one table
                      list[str]     → restrict to multiple tables (new)
        pg_conn:      Optional existing connection.

    Preference order for embeddings:
        embedding_768 (Ollama) preferred; embedding_384 (MiniLM) fallback.
        Both are searched via UNION, deduplicated by (table_name, row_id),
        keeping the higher-quality 768 result when both exist for a row.
        Final results are re-sorted by similarity descending so the most
        relevant rows always come first regardless of which model found them.
    """
    query_vec_768 = None
    query_vec_384 = None

    ollama_result = _embed_ollama([query])
    if ollama_result:
        query_vec_768 = ollama_result[0]

    minilm_result = _embed_minilm([query])
    if minilm_result:
        query_vec_384 = minilm_result[0]

    if not query_vec_768 and not query_vec_384:
        raise RuntimeError("Both Ollama and MiniLM failed to embed the query.")

    # Normalise table_filter → list or None
    if isinstance(table_filter, str) and table_filter.strip():
        tables = [table_filter.strip()]
    elif isinstance(table_filter, list) and table_filter:
        tables = [t.strip() for t in table_filter if t and t.strip()]
    else:
        tables = []

    # Build the WHERE clause for table filtering
    # Using ANY(%s::text[]) so psycopg2 can pass a Python list directly
    if tables:
        table_clause = "AND table_name = ANY(%(tables)s)"
    else:
        table_clause = ""

    close_conn = pg_conn is None
    if close_conn:
        pg_conn = _get_pg_conn()

    try:
        cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            # fetch top_k * 2 from each arm of the UNION so deduplication
            # doesn't accidentally drop relevant results
            params = {
                "tables": tables if tables else None,
                "top_k_inner": top_k * 2,
                "top_k": top_k,
            }
            parts = []

            # IMPORTANT: PostgreSQL does not allow ORDER BY / LIMIT inside
            # a UNION branch directly.  Each branch must be wrapped in its
            # own subquery (SELECT * FROM (...) sub) so the outer UNION ALL
            # can combine them legally.
            if query_vec_768:
                params["q768"] = str(query_vec_768)
                parts.append(f"""
                    SELECT * FROM (
                        SELECT table_name, row_id, row_text, row_data,
                               1 - (embedding_768 <=> %(q768)s::vector) AS similarity,
                               'ollama_768' AS source
                        FROM row_embeddings
                        WHERE embedding_768 IS NOT NULL {table_clause}
                        ORDER BY embedding_768 <=> %(q768)s::vector
                        LIMIT %(top_k_inner)s
                    ) sub768
                """)

            if query_vec_384:
                params["q384"] = str(query_vec_384)
                parts.append(f"""
                    SELECT * FROM (
                        SELECT table_name, row_id, row_text, row_data,
                               1 - (embedding_384 <=> %(q384)s::vector) AS similarity,
                               'minilm_384' AS source
                        FROM row_embeddings
                        WHERE embedding_384 IS NOT NULL {table_clause}
                        ORDER BY embedding_384 <=> %(q384)s::vector
                        LIMIT %(top_k_inner)s
                    ) sub384
                """)

            union_sql = " UNION ALL ".join(parts)

            # DISTINCT ON deduplicates by (table_name, row_id).
            # Within each pair the CASE gives 768 priority.
            # The outer ORDER BY similarity DESC re-ranks globally by relevance.
            final_sql = f"""
                SELECT table_name, row_id, row_text, row_data, similarity, source
                FROM (
                    SELECT DISTINCT ON (table_name, row_id)
                        table_name, row_id, row_text, row_data, similarity, source
                    FROM ({union_sql}) combined
                    ORDER BY table_name, row_id,
                             CASE source WHEN 'ollama_768' THEN 0 ELSE 1 END,
                             similarity DESC
                ) deduped
                ORDER BY similarity DESC
                LIMIT %(top_k)s
            """

            cur.execute(final_sql, params)
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
    finally:
        if close_conn:
            pg_conn.close()