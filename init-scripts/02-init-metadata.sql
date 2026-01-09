-- Add metadata column if not exists
ALTER TABLE minio_data_catalog 
ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::JSONB;