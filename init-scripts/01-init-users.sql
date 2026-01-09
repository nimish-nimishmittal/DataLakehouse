-- Create users table if not exists
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',  -- 'admin' or 'user'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add uploaded_by to minio_data_catalog (foreign key to users)
ALTER TABLE minio_data_catalog 
ADD COLUMN IF NOT EXISTS uploaded_by INTEGER REFERENCES users(id) ON DELETE SET NULL;

-- Create default admin user (change password_hash to a real one later)
INSERT INTO users (username, password_hash, role) 
VALUES ('admin', '$2b$12$KlASdBHHI0tWJVLrzD3gN.joop5qqXDqgNNB.c8G35J2MR/b82Mgy', 'admin')  -- Password: 'adminpassword' (hashed with bcrypt)
ON CONFLICT (username) DO NOTHING;