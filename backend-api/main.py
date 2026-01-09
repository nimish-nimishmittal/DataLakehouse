from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Optional, Annotated
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from datetime import datetime, timedelta
import os
import io
from werkzeug.utils import secure_filename
from passlib.context import CryptContext
from jose import JWTError, jwt

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Lakehouse Admin API")

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for DEV MODE ONLY !! must be changed when in prod !!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        database=os.getenv('POSTGRES_DB', 'lakehouse_db'),
        user=os.getenv('POSTGRES_USER', 'lakehouse_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'lakehouse_pass'),
        cursor_factory=RealDictCursor
    )

# MinIO connection
minio_client = Minio(
    os.getenv('MINIO_ENDPOINT', 'minio:9000'),
    access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
    secure=False
)

BUCKET = os.getenv('MINIO_BUCKET', 'lakehouse-data')
ALLOWED_EXT = {
    'csv', 'json', 'parquet', 'pdf', 'docx', 'doc',
    'png', 'jpg', 'jpeg', 'tiff', 'pptx', 'ppt'
}

SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-super-secret-key-here')  # From env
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # Token lifetime
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# NEW: Pydantic Models
class User(BaseModel):
    id: int
    username: str
    role: str

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class ChangePassword(BaseModel):
    old_password: str
    new_password: str

class RegisterUser(BaseModel):
    username: str
    password: str

# NEW: Helper to get user from DB
def get_user_from_db(username: str):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username = %s", (username,))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

# NEW: Authenticate user (check password)
def authenticate_user(username: str, password: str):
    user = get_user_from_db(username)
    if not user or not pwd_context.verify(password, user['password_hash']):
        return False
    return user

# NEW: Create JWT token
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# NEW: Dependency to get current user from token
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user_from_db(username)
    if user is None:
        raise credentials_exception
    return User(id=user['id'], username=user['username'], role=user['role'])

# NEW: Role Checker Dependency
class RoleChecker:
    def __init__(self, allowed_roles: List[str]):
        self.allowed_roles = allowed_roles

    def __call__(self, user: Annotated[User, Depends(get_current_user)]):
        if user.role not in self.allowed_roles:
            raise HTTPException(status_code=403, detail="Operation not permitted")
        return user

# Allow only admins for certain endpoints
admin_only = RoleChecker(["admin"])

# NEW: Login Endpoint
@app.post("/api/auth/login", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['username'], "id": user['id'], "role": user['role']},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token}

@app.post("/api/auth/change-password")
async def change_password(
    payload: ChangePassword,
    current_user: Annotated[User, Depends(get_current_user)]
):
    """Change the current user's password"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        # Fetch current user's hash
        cursor.execute(
            "SELECT password_hash FROM users WHERE id = %s",
            (current_user.id,)
        )
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Verify old password
        if not pwd_context.verify(payload.old_password, user['password_hash']):
            raise HTTPException(status_code=400, detail="Incorrect old password")
        
        # Hash new password
        new_hash = pwd_context.hash(payload.new_password)
        
        # Update DB
        cursor.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (new_hash, current_user.id)
        )
        conn.commit()
        
        return {"status": "Password changed successfully"}
    
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Password change failed: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()

# NEW: Register Endpoint (optional, for creating users)
@app.post("/api/auth/register")
async def register_user(user: RegisterUser):
    hashed_password = pwd_context.hash(user.password)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'user')",
            (user.username, hashed_password)
        )
        conn.commit()
        return {"status": "User created successfully"}
    except psycopg2.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=409, detail="Username already taken")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Registration failed")
    finally:
        cursor.close()
        conn.close()

# ==================== DASHBOARD METRICS ====================
@app.get("/api/dashboard/metrics")
async def get_dashboard_metrics(current_user: Annotated[User, Depends(get_current_user)]):
    """Get key metrics for dashboard cards"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Build base conditions for RBAC
    conditions = []
    params = []
    if current_user.role != 'admin':
        conditions.append("uploaded_by = %s")
        params.append(current_user.id)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    base_params = params.copy()

    try:
        # Total documents
        cursor.execute(f"SELECT COUNT(*) as total FROM minio_data_catalog {where_clause}", base_params)
        total_docs = cursor.fetchone()['total']
        
        # Processed today
        today_params = base_params.copy()
        today_clause = where_clause + (" AND " if where_clause else "WHERE ") + "DATE(created_at) = CURRENT_DATE"
        cursor.execute(f"SELECT COUNT(*) as today FROM minio_data_catalog {today_clause}", today_params)
        processed_today = cursor.fetchone()['today']
        
        # Files in raw (fixed!)
        raw_params = base_params.copy()
        raw_clause = where_clause + (" AND " if where_clause else "WHERE ") + "object_name LIKE 'raw/%'"
        if raw_params:
            cursor.execute(
                f"SELECT COUNT(*) as raw_count FROM minio_data_catalog {raw_clause}",
                raw_params
            )
        else:
            cursor.execute(
                f"SELECT COUNT(*) as raw_count FROM minio_data_catalog {raw_clause}"
            )
        raw_count = cursor.fetchone()['raw_count']
        
        # Total storage used (in GB)
        storage_params = base_params.copy()
        cursor.execute(f"SELECT COALESCE(SUM(object_size), 0) as total_size FROM minio_data_catalog {where_clause}", storage_params)
        total_bytes = cursor.fetchone()['total_size'] or 0
        total_storage_gb = round(total_bytes / (1024**3), 2)
        
        # Files by format
        format_params = base_params.copy()
        format_clause = where_clause + (" AND " if where_clause else "WHERE ") + "file_format IS NOT NULL"
        cursor.execute(f"""
            SELECT file_format, COUNT(*) as count 
            FROM minio_data_catalog 
            {format_clause}
            GROUP BY file_format
            ORDER BY count DESC
        """, format_params)
        files_by_format = cursor.fetchall()
        
        # Recent activity
        recent_params = base_params.copy()
        cursor.execute(f"""
            SELECT object_name, file_format, created_at, object_size
            FROM minio_data_catalog
            {where_clause}
            ORDER BY created_at DESC
            LIMIT 5
        """, recent_params)
        recent_activity = cursor.fetchall()
        
        return {
            "total_documents": total_docs,
            "processed_today": processed_today,
            "files_in_raw": raw_count,
            "total_storage_gb": total_storage_gb,
            "files_by_format": [dict(row) for row in files_by_format],
            "recent_activity": [dict(row) for row in recent_activity]
        }
        
    except Exception as e:
        logger.exception("Metrics query failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# ==================== FILE MANAGEMENT ====================

@app.get("/api/files")
async def list_files(
    current_user: Annotated[User, Depends(get_current_user)],  # Requires login
    limit: int = 50,
    offset: int = 0,
    format: Optional[str] = None,
    search: Optional[str] = None
):
    """List files with pagination and filtering – respects user ownership"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        where_clauses = []
        params = []
        
        # RBAC: regular users only see their own files
        if current_user.role != "admin":
            where_clauses.append("uploaded_by = %s")
            params.append(current_user.id)
        
        if format:
            where_clauses.append("file_format = %s")
            params.append(format)
        
        if search:
            where_clauses.append("object_name ILIKE %s")
            params.append(f"%{search}%")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Get total count
        count_params = params.copy()
        cursor.execute(f"SELECT COUNT(*) as total FROM minio_data_catalog WHERE {where_sql}", count_params)
        total = cursor.fetchone()['total']
        
        # Get paginated results
        query_params = params.copy()
        cursor.execute(f"""
            SELECT catalog_id, bucket_name, object_name,object_size,
                file_format,
                row_count,
                text_extracted,
                content_hash,
                created_at,
                last_modified,
                metadata
            FROM minio_data_catalog
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, query_params + [limit, offset])
        
        files = cursor.fetchall()
        
        return {
            "total": total,
            "files": files,
            "limit": limit,
            "offset": offset
        }
        
    finally:
        cursor.close()
        conn.close()

@app.get("/api/files/{catalog_id}")
async def get_file_details(catalog_id: int):
    """Get detailed information about a specific file"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT * FROM minio_data_catalog WHERE catalog_id = %s
        """, (catalog_id,))
        
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Check if file exists in MinIO
        try:
            stat = minio_client.stat_object(file_info['bucket_name'], file_info['object_name'])
            minio_exists = True
            minio_info = {
                "size": stat.size,
                "last_modified": stat.last_modified.isoformat(),
                "etag": stat.etag
            }
        except:
            minio_exists = False
            minio_info = None
        
        return {
            "catalog": file_info,
            "minio_exists": minio_exists,
            "minio_info": minio_info
        }
        
    finally:
        cursor.close()
        conn.close()

@app.delete("/api/files/{catalog_id}")
async def delete_file(catalog_id: int):
    """Delete a file from both catalog and MinIO"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Get file info
        cursor.execute("SELECT bucket_name, object_name FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Delete from MinIO
        try:
            minio_client.remove_object(file_info['bucket_name'], file_info['object_name'])
        except Exception as e:
            print(f"MinIO delete failed: {e}")
        
        # Delete from catalog
        cursor.execute("DELETE FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        conn.commit()
        
        return {"status": "deleted", "catalog_id": catalog_id}
        
    finally:
        cursor.close()
        conn.close()

# ==================== FILE UPLOAD ====================

@app.post("/api/upload")
async def upload_file(current_user: Annotated[User, Depends(get_current_user)] ,file: UploadFile = File(...)):
    """Upload a file to MinIO raw bucket"""
    
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    # Validate extension
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in ALLOWED_EXT:
        raise HTTPException(status_code=400, detail=f"File type .{ext} not supported")
    
    filename = secure_filename(file.filename)
    
    # Determine file type category (same as before)
    file_type_map = {
        'csv': 'structured', 'json': 'structured', 'parquet': 'structured',
        'pdf': 'pdf', 'docx': 'docx', 'doc': 'docx',
        'png': 'image', 'jpg': 'image', 'jpeg': 'image', 'tiff': 'image',
        'pptx': 'ppt', 'ppt': 'ppt'
    }
    file_type = file_type_map.get(ext, 'other')
    
    object_name = f"raw/{file_type}/{filename}"
    
    # Read file content
    content = await file.read()
    
    try:
        # Upload to MinIO
        minio_client.put_object(
            BUCKET,
            object_name,
            io.BytesIO(content),
            length=len(content),
            content_type=file.content_type or 'application/octet-stream'
        )
        
        # Update catalog with uploaded_by
        update_catalog(
            bucket=BUCKET,
            object_name=object_name,
            object_size=len(content),
            file_format=file_type,
            uploaded_by=current_user.id
        )
        
        return {
            "status": "success",
            "object_name": object_name,
            "size": len(content),
            "message": "File uploaded successfully. Processing will begin shortly."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

# ==================== SEARCH ====================

@app.get("/api/search")
async def search_documents(query: str, limit: int = 20):
    """Search across extracted text in unstructured_documents"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                id,
                object_name,
                file_type,
                LEFT(text_content, 200) as preview,
                created_at
            FROM unstructured_documents
            WHERE text_content ILIKE %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (f"%{query}%", limit))
        
        results = cursor.fetchall()
        
        return {
            "query": query,
            "count": len(results),
            "results": results
        }
        
    finally:
        cursor.close()
        conn.close()

# ==================== STATS ====================

@app.get("/api/stats/storage")
async def get_storage_stats(current_user: Annotated[User, Depends(get_current_user)]):
    conn = get_db()
    cursor = conn.cursor()
    
    where_clause = ""
    params = []
    if current_user.role != 'admin':
        where_clause = "WHERE uploaded_by = %s"
        params = [current_user.id]
    
    try:
        cursor.execute(f"""
            SELECT 
                file_format,
                COUNT(*) as file_count,
                SUM(object_size) as total_size,
                AVG(object_size) as avg_size
            FROM minio_data_catalog
            WHERE file_format IS NOT NULL { 'AND uploaded_by = %s' if current_user.role != 'admin' else '' }
            GROUP BY file_format
            ORDER BY total_size DESC
        """, params if current_user.role != 'admin' else None)
        
        stats = cursor.fetchall()
        
        return {
            "storage_by_type": [
                {
                    **row,
                    "total_size_mb": round((row['total_size'] or 0) / (1024**2), 2),
                    "avg_size_kb": round((row['avg_size'] or 0) / 1024, 2)
                }
                for row in stats
            ]
        }
        
    finally:
        cursor.close()
        conn.close()

@app.get("/api/stats/processing")
async def get_processing_stats(current_user: Annotated[User, Depends(get_current_user)]):
    conn = get_db()
    cursor = conn.cursor()
    
    where_clause = ""
    params = []
    if current_user.role != 'admin':
        where_clause = "AND uploaded_by = %s"
        params = [current_user.id]
    
    try:
        # Last 30 days trend (filtered)
        trend_params = params.copy()
        cursor.execute(f"""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as count,
                file_format
            FROM minio_data_catalog
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' {where_clause}
            GROUP BY DATE(created_at), file_format
            ORDER BY date DESC, file_format
        """, trend_params)
        
        trend = cursor.fetchall()
        
        # Processing success rate (text extraction) (filtered)
        extraction_params = params.copy()
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN text_extracted THEN 1 ELSE 0 END) as extracted
            FROM minio_data_catalog
            WHERE file_format IN ('pdf', 'docx', 'pptx') {where_clause}
        """, extraction_params)
        
        extraction_stats = cursor.fetchone()
        
        return {
            "daily_trend": trend,
            "extraction_rate": {
                "total": extraction_stats['total'],
                "extracted": extraction_stats['extracted'],
                "rate": round((extraction_stats['extracted'] / extraction_stats['total'] * 100) if extraction_stats['total'] > 0 else 0, 2)
            }
        }
        
    finally:
        cursor.close()
        conn.close()

# ==================== HEALTH CHECK ====================

@app.get("/api/health")
async def health_check():
    """Check health of all services"""
    health = {
        "api": "ok",
        "postgres": "unknown",
        "minio": "unknown"
    }
    
    # Check PostgreSQL
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        health["postgres"] = "ok"
    except Exception as e:
        health["postgres"] = f"error: {str(e)}"
    
    # Check MinIO
    try:
        minio_client.bucket_exists(BUCKET)
        health["minio"] = "ok"
    except Exception as e:
        health["minio"] = f"error: {str(e)}"
    
    return health

def update_catalog(bucket, object_name, object_size=None, file_format=None, uploaded_by=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minio_data_catalog (
                catalog_id SERIAL PRIMARY KEY,
                bucket_name TEXT NOT NULL,
                object_name TEXT NOT NULL,
                object_size BIGINT,
                file_format TEXT,
                row_count INTEGER,
                text_extracted BOOLEAN DEFAULT FALSE,
                last_modified TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uploaded_by INTEGER,
                UNIQUE(bucket_name, object_name)
            )
        """)
        cursor.execute("""
            INSERT INTO minio_data_catalog (bucket_name, object_name, object_size, file_format, uploaded_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bucket_name, object_name) DO UPDATE
            SET object_size = EXCLUDED.object_size,
                file_format = EXCLUDED.file_format,
                uploaded_by = EXCLUDED.uploaded_by,
                last_modified = CURRENT_TIMESTAMP
        """, (bucket, object_name, object_size, file_format, uploaded_by))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Catalog update failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    