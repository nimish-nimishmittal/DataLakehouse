-- ============================================================
-- 03_setup_vector_search.sql
-- Run this once to bootstrap the entire vector search system.
-- Connects to your existing lakehouse_db.
-- ============================================================

-- ── 1. Enable pgvector extension ────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;


-- ── 2. embedding_queue ──────────────────────────────────────
-- Every INSERT/UPDATE/DELETE on any watched table writes a
-- lightweight event here.  The Airflow DAG reads this table,
-- generates embeddings, and marks rows as done.
CREATE TABLE IF NOT EXISTS embedding_queue (
    id            BIGSERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    row_id        TEXT        NOT NULL,   -- stringified PK value(s)
    operation     TEXT        NOT NULL,   -- INSERT | UPDATE | DELETE
    row_data      JSONB,                  -- full row snapshot (NULL on DELETE)
    status        TEXT        NOT NULL DEFAULT 'pending',  -- pending | done | error
    error_msg     TEXT,
    created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_embedding_queue_status
    ON embedding_queue (status, created_at);


-- ── 3. row_embeddings ───────────────────────────────────────
-- One row per unique (table_name, row_id).  Holds the vector,
-- the serialised text used to create it, and the source data.
CREATE TABLE IF NOT EXISTS row_embeddings (
    id            BIGSERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    row_id        TEXT        NOT NULL,   -- same format as embedding_queue.row_id
    row_text      TEXT        NOT NULL,   -- human-readable serialisation of the row
    embedding     vector(384),            -- all-MiniLM-L6-v2 outputs 384 dims
    row_data      JSONB,                  -- snapshot of the row at embed time
    model_name    TEXT        NOT NULL DEFAULT 'all-MiniLM-L6-v2',
    created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_name, row_id)
);

CREATE INDEX idx_row_embeddings_vector
ON row_embeddings
USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_row_embeddings_table
    ON row_embeddings (table_name);


-- ── 4. Trigger function ──────────────────────────────────────
-- A single PL/pgSQL function shared by all watched tables.
-- It serialises the changed row into embedding_queue.
-- The Airflow DAG reads the queue asynchronously — the trigger
-- is kept as lightweight as possible (no network calls, no Python).
CREATE OR REPLACE FUNCTION fn_queue_embedding_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_id  TEXT;
    v_row_data JSONB;
BEGIN
    -- Build a stable row identifier from the first column named
    -- 'id' if it exists, otherwise use ctid (physical row address).
    -- For DELETE we use OLD, for INSERT/UPDATE we use NEW.
    IF TG_OP = 'DELETE' THEN
        BEGIN
            v_row_id   := OLD.id::TEXT;
        EXCEPTION WHEN undefined_column THEN
            v_row_id   := OLD.ctid::TEXT;
        END;
        v_row_data := NULL;  -- row is gone, nothing to embed; embedder will delete it

        INSERT INTO embedding_queue (table_name, row_id, operation, row_data)
        VALUES (TG_TABLE_NAME, v_row_id, 'DELETE', v_row_data);

        RETURN OLD;
    ELSE
        BEGIN
            v_row_id   := NEW.id::TEXT;
        EXCEPTION WHEN undefined_column THEN
            v_row_id   := NEW.ctid::TEXT;
        END;
        v_row_data := to_jsonb(NEW);

        INSERT INTO embedding_queue (table_name, row_id, operation, row_data)
        VALUES (TG_TABLE_NAME, v_row_id, TG_OP, v_row_data);

        RETURN NEW;
    END IF;
END;
$$;


-- ── 5. attach_embedding_trigger() helper ────────────────────
-- Call this once per table you want to watch.
-- The Airflow DAG also calls it automatically for new tables.
CREATE OR REPLACE FUNCTION attach_embedding_trigger(p_table TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_trigger_name TEXT := 'trg_embed_' || p_table;
BEGIN
    -- Skip if trigger already exists on this table
    IF EXISTS (
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_name = v_trigger_name
          AND event_object_table = p_table
    ) THEN
        RETURN;
    END IF;

    EXECUTE format(
        'CREATE TRIGGER %I
         AFTER INSERT OR UPDATE OR DELETE
         ON %I
         FOR EACH ROW
         EXECUTE FUNCTION fn_queue_embedding_event()',
        v_trigger_name, p_table
    );
END;
$$;


-- ── 6. Attach triggers to ALL existing data_* tables now ────
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE 'data_%'   -- your pipeline-generated tables
    LOOP
        PERFORM attach_embedding_trigger(tbl.tablename);
    END LOOP;

    -- Also watch the unstructured stores
    PERFORM attach_embedding_trigger('unstructured_documents');
    PERFORM attach_embedding_trigger('unstructured_images');
    PERFORM attach_embedding_trigger('minio_data_catalog');
END;
$$;