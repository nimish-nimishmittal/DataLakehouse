# python-etl/uploader_app.py
import os
import io
import logging
import psycopg2
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from minio import Minio
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from bcrypt import hashpw, gensalt, checkpw
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv('MINIO_BUCKET', 'lakehouse-data')
ALLOWED_EXT = {
    'csv': 'structured',
    'json': 'structured',
    'parquet': 'structured',
    'pdf': 'pdf',
    'docx': 'docx',
    'doc': 'docx',
    'png': 'image',
    'jpg': 'image',
    'jpeg': 'image',
    'tiff': 'image',
}
MAX_CONTENT_LENGTH = 200 * 1024 * 1024  # 200MB example

app = Flask(__name__)

from flask_cors import CORS
CORS(app)

app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET', 'super-secret-key')  # From env/docker-compose
jwt = JWTManager(app)

# Register the search blueprint — provides /search, /search/tables, /search/status
from search_api import search_bp
app.register_blueprint(search_bp)

# Register the RAG blueprint — provides /rag/query (SSE stream), /rag/status, /rag/models
from rag_api import rag_bp
app.register_blueprint(rag_bp)

minio_client = Minio(
    os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
    secure=False
)

import time

pg_conn = None
while True:
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'lakehouse_db'),
            user=os.getenv('POSTGRES_USER', 'lakehouse_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'lakehouse_pass')
        )
        logger.info("Connected to Postgres successfully.")
        break
    except Exception as e:
        logger.warning(f"Waiting for Postgres... error: {e}")
        time.sleep(2)
pg_conn.autocommit = False

def resolve_uploaded_by(jwt_identity):
    """
    Normalize JWT identity to user_id (int).
    Supports:
      - dict identity: {'id': ..., 'username': ..., 'role': ...}
      - string identity: 'username'
    """
    # Case 1: New-style dict identity
    if isinstance(jwt_identity, dict):
        if 'id' not in jwt_identity:
            raise ValueError("JWT identity dict missing 'id'")
        return jwt_identity['id']

    # Case 2: Legacy string identity (username)
    if isinstance(jwt_identity, str):
        user = get_user(jwt_identity)
        if not user:
            raise ValueError("JWT identity username not found in DB")
        return user["id"]  # user_id

    # Case 3: Anything else → reject
    raise ValueError(f"Unsupported JWT identity type: {type(jwt_identity)}")

# Helper: Get user from DB
def get_user(username):
    cursor = pg_conn.cursor()
    try:
        cursor.execute("SELECT id, password_hash, role FROM users WHERE username = %s", (username,))
        return cursor.fetchone()
    finally:
        cursor.close()

# Login endpoint to get JWT
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'error': 'Username and password required'}), 400

    username = data.get('username')
    password = data.get('password')

    user = get_user(username)  # Returns (id, password_hash, role) or None
    if not user or not checkpw(password.encode('utf-8'), user[1].encode('utf-8')):
        return jsonify({'error': 'Invalid credentials'}), 401

    # Create token with dict identity
    access_token = create_access_token(
        identity={
            'id': user[0],
            'username': username,
            'role': user[2] or 'user'  # fallback if role is None
        }
    )

    return jsonify({'token': access_token}), 200

# Registration endpoint (optional, for users to sign up)
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'error': 'Username and password required'}), 400

    username = data.get('username')
    password = data.get('password')

    # Hash and convert to STRING
    hashed_bytes = hashpw(password.encode('utf-8'), gensalt())
    hashed_str = hashed_bytes.decode('utf-8')  # ← THIS IS THE FIX

    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'user')",
            (username, hashed_str)  # ← Pass string, not bytes
        )
        pg_conn.commit()
        return jsonify({'status': 'User created'}), 201
    except psycopg2.IntegrityError:
        pg_conn.rollback()
        return jsonify({'error': 'Username already taken'}), 409
    except Exception as e:
        pg_conn.rollback()
        logger.exception("Registration failed")
        return jsonify({'error': 'Registration failed'}), 500
    finally:
        cursor.close()

# ── Duplicate filename resolver ───────────────────────────────
def resolve_unique_object_name(bucket: str, desired_object_name: str) -> str:
    """
    Return a unique object name in MinIO by appending (1), (2), (3)...
    to the stem if the desired name already exists.

    Examples:
      raw/report.pdf          → raw/report.pdf          (if free)
      raw/report.pdf (taken)  → raw/report(1).pdf
      raw/report(1).pdf taken → raw/report(2).pdf
      ...
    """
    try:
        minio_client.stat_object(bucket, desired_object_name)
        # Object exists — need a new name
    except Exception:
        # stat_object throws when object does NOT exist — name is free
        return desired_object_name

    # Split stem and extension
    if '.' in desired_object_name.split('/')[-1]:
        prefix = desired_object_name.rsplit('.', 1)[0]   # e.g. "raw/report"
        ext    = '.' + desired_object_name.rsplit('.', 1)[1]  # e.g. ".pdf"
    else:
        prefix = desired_object_name
        ext    = ''

    counter = 1
    while True:
        candidate = f"{prefix}({counter}){ext}"
        try:
            minio_client.stat_object(bucket, candidate)
            counter += 1  # That one also exists, keep going
        except Exception:
            return candidate  # Free — use it


# ── Catalog updater ───────────────────────────────────────────
def update_catalog(bucket, object_name, object_size=None, file_format=None,
                   row_count=None, text_extracted=False, uploaded_by=None,
                   metadata: dict = None):
    """
    Upsert a row into minio_data_catalog.
    Table must already exist (created by init SQL script).
    No inline CREATE TABLE — that belongs in the init script only.
    content_hash has been intentionally removed from this system.
    """
    cursor = pg_conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO minio_data_catalog
                (bucket_name, object_name, object_size, file_format,
                 row_count, text_extracted, uploaded_by, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (bucket_name, object_name) DO UPDATE
            SET object_size    = EXCLUDED.object_size,
                file_format    = EXCLUDED.file_format,
                row_count      = EXCLUDED.row_count,
                text_extracted = EXCLUDED.text_extracted,
                uploaded_by    = EXCLUDED.uploaded_by,
                metadata       = EXCLUDED.metadata,
                last_modified  = CURRENT_TIMESTAMP
            """,
            (bucket, object_name, object_size, file_format,
             row_count, text_extracted, uploaded_by, json.dumps(metadata or {})),
        )
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        logger.exception("Failed updating catalog")
        raise
    finally:
        cursor.close()


# ── Upload endpoint ───────────────────────────────────────────
@app.route('/upload', methods=['POST'])
@jwt_required()
def upload_file():
    jwt_identity = get_jwt_identity()

    try:
        uploaded_by = resolve_uploaded_by(jwt_identity)
    except ValueError as e:
        logger.error(f"JWT identity resolution failed: {e}")
        return jsonify({"error": "Invalid authentication context"}), 401

    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename  = secure_filename(file.filename)
    ext       = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    file_type = ALLOWED_EXT.get(ext)

    if not file_type:
        return jsonify({"error": f"Extension .{ext} not supported"}), 400

    data = file.read()

    # ── Resolve unique object name (deduplication) ────────────
    # If raw/report.pdf already exists, this returns raw/report(1).pdf etc.
    desired_name  = f"raw/{filename}"
    object_name   = resolve_unique_object_name(BUCKET, desired_name)

    if object_name != desired_name:
        logger.info(
            f"Duplicate filename detected — renamed '{desired_name}' → '{object_name}'"
        )

    try:
        minio_client.put_object(
            BUCKET,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=file.content_type,
            metadata={"x-amz-meta-uploaded-by": str(uploaded_by)} if uploaded_by else {}
        )
        logger.info(f"Uploaded file to MinIO at {object_name}")

        import magic
        from datetime import datetime
        mime_type      = magic.from_buffer(data, mime=True) if data else file.content_type
        basic_metadata = {
            'original_filename': filename,
            'stored_as':         object_name,          # reflects rename if any
            'mime_type':         mime_type,
            'upload_time':       datetime.utcnow().isoformat(),
            'source':            'api_upload',
        }
        if object_name != desired_name:
            basic_metadata['renamed_from'] = desired_name  # audit trail

        update_catalog(
            BUCKET,
            object_name,
            object_size=len(data),
            file_format=file_type,
            uploaded_by=uploaded_by,
            metadata=basic_metadata,
        )
    except Exception:
        logger.exception("Failed uploading to MinIO")
        return jsonify({"error": "upload failed"}), 500

    return jsonify({
        "status":      "ok",
        "object":      object_name,
        "original":    filename,
        "renamed":     object_name != desired_name,
    }), 200


if __name__ == "__main__":
    # ensure bucket exists
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
    app.run(host="0.0.0.0", port=int(os.getenv('UPLOAD_PORT', 5000)))