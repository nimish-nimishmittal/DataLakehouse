-- init-scripts/02_init_minio_data_catalog.sql
-- ============================================================
-- minio_data_catalog — complete init script
--
-- Matches the live schema exactly:
--   catalog_id, bucket_name, object_name, object_size,
--   file_format, row_count, text_extracted, content_hash,
--   last_modified, created_at, uploaded_by (FK → users),
--   metadata JSONB
--
-- Also attaches the embedding trigger so new catalog rows
-- are automatically queued for vector embedding.
--
-- Safe to run repeatedly (all statements are idempotent).
-- ============================================================

-- users table must already exist (created in 01_init.sql).
-- The FK below will fail if users does not exist.

CREATE TABLE IF NOT EXISTS minio_data_catalog (
    catalog_id     SERIAL       PRIMARY KEY,
    bucket_name    TEXT         NOT NULL,
    object_name    TEXT         NOT NULL,
    object_size    BIGINT,
    file_format    TEXT,
    row_count      INTEGER,
    text_extracted BOOLEAN      DEFAULT FALSE,
    last_modified  TIMESTAMP,
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    uploaded_by    INTEGER      REFERENCES users(id) ON DELETE SET NULL,
    metadata       JSONB        DEFAULT '{}'::JSONB,

    CONSTRAINT minio_data_catalog_bucket_name_object_name_key
        UNIQUE (bucket_name, object_name)
);

-- ── Indexes ───────────────────────────────────────────────────
-- Speeds up catalog lookups by bucket, format, uploader, and hash
CREATE INDEX IF NOT EXISTS idx_mdc_bucket_name
    ON minio_data_catalog (bucket_name);

CREATE INDEX IF NOT EXISTS idx_mdc_file_format
    ON minio_data_catalog (file_format);

CREATE INDEX IF NOT EXISTS idx_mdc_uploaded_by
    ON minio_data_catalog (uploaded_by);

CREATE INDEX IF NOT EXISTS idx_mdc_content_hash
    ON minio_data_catalog (content_hash)
    WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mdc_created_at
    ON minio_data_catalog (created_at DESC);

-- ── Embedding trigger ─────────────────────────────────────────
-- fn_queue_embedding_event() must already exist (created in
-- 03_setup_vector_search.sql or equivalent).
-- The DO $$ block below is idempotent: drops the trigger first
-- so re-running this script never raises a "already exists" error.

DO $$
BEGIN
    -- Drop if exists so re-runs are safe
    DROP TRIGGER IF EXISTS trg_embed_minio_data_catalog
        ON minio_data_catalog;

    -- Only attach if the trigger function has been created already
    IF EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'fn_queue_embedding_event'
    ) THEN
        CREATE TRIGGER trg_embed_minio_data_catalog
            AFTER INSERT OR UPDATE OR DELETE
            ON minio_data_catalog
            FOR EACH ROW
            EXECUTE FUNCTION fn_queue_embedding_event();

        RAISE NOTICE 'Embedding trigger attached to minio_data_catalog';
    ELSE
        RAISE NOTICE 'fn_queue_embedding_event not found — embedding trigger NOT attached. Run vector search init first.';
    END IF;
END;
$$;