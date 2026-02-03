from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Optional, Annotated, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from datetime import datetime, timedelta
import os
import io
import json
import logging
import requests
from requests.auth import HTTPBasicAuth
import magic
from werkzeug.utils import secure_filename
from passlib.context import CryptContext
from jose import JWTError, jwt

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LakehouseAPI")

# API Metadata for Swagger UI
tags_metadata = [
    {"name": "Authentication", "description": "Identity and Access Management"},
    {"name": "Dashboard", "description": "Metrics and visualization data"},
    {"name": "Files", "description": "Data catalog and object storage operations"},
    {"name": "Jobs", "description": "Airflow ETL pipeline monitoring"},
    {"name": "Admin", "description": "Privileged system operations"},
    {"name": "Health", "description": "System readiness and status"},
]

app = FastAPI(
    title="Data Lakehouse Enterprise API",
    description="Unified API gateway for the Enterprise Data Lakehouse including Object Storage, Catalog, and ETL Monitoring.",
    version="2.0.0",
    openapi_tags=tags_metadata,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

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

SECRET_KEY = os.getenv('JWT_SECRET_KEY', '0e22bafb4673f430ab2dbf83192efc50bfc70fecef2da81a9abea84daec736bc')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # Increased for enterprise session
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Airflow Config
AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://airflow_lakehouse:8080/api/v1')
AIRFLOW_USER = os.getenv('AIRFLOW_USER', 'admin')
AIRFLOW_PASS = os.getenv('AIRFLOW_PASS', 'admin')

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

# NEW: Audit Helper
def log_audit(user_id: Optional[int], action: str, details: str = None, ip: str = None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO audit_logs (user_id, action, details, ip_address) VALUES (%s, %s, %s, %s)",
            (user_id, action, details, ip)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Audit log failed: {e}")
    finally:
        cursor.close()
        conn.close()

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
    log_audit(user['id'], "LOGIN", f"User {user['username']} logged in")
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

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        # Total documents
        cursor.execute(f"SELECT COUNT(*) as total FROM minio_data_catalog {where_clause}", params)
        total_docs = cursor.fetchone().get('total', 0)

        # Processed today
        today_clause = where_clause + (" AND " if where_clause else "WHERE ") + "DATE(created_at) = CURRENT_DATE"
        cursor.execute(f"SELECT COUNT(*) as today FROM minio_data_catalog {today_clause}", params)
        processed_today = cursor.fetchone().get('today', 0)

        # Files in raw
        raw_clause = where_clause + (" AND " if where_clause else "WHERE ") + "object_name LIKE 'raw/%'"
        cursor.execute(f"SELECT COUNT(*) as raw_count FROM minio_data_catalog {raw_clause}", params)
        raw_count = cursor.fetchone().get('raw_count', 0)

        # Total storage used (in GB)
        cursor.execute(f"SELECT COALESCE(SUM(object_size), 0) as total_size FROM minio_data_catalog {where_clause}", params)
        total_bytes = cursor.fetchone().get('total_size', 0)
        total_storage_gb = round(total_bytes / (1024**3), 2)

        # Files by format
        format_clause = where_clause + (" AND " if where_clause else "WHERE ") + "file_format IS NOT NULL"
        cursor.execute(f"""
            SELECT file_format, COUNT(*) as count 
            FROM minio_data_catalog 
            {format_clause}
            GROUP BY file_format
            ORDER BY count DESC
        """, params)
        files_by_format = cursor.fetchall()

        # Recent activity
        cursor.execute(f"""
            SELECT object_name, file_format, created_at, object_size
            FROM minio_data_catalog
            {where_clause}
            ORDER BY created_at DESC
            LIMIT 5
        """, params)
        recent_activity = cursor.fetchall()

        # User count for admins
        total_users = 0
        if current_user.role == 'admin':
            cursor.execute("SELECT COUNT(*) as count FROM users")
            total_users = cursor.fetchone().get('count', 0)

        return {
            "total_documents": total_docs,
            "processed_today": processed_today,
            "files_in_raw": raw_count,
            "total_storage_gb": total_storage_gb,
            "total_users": total_users,
            "files_by_format": [dict(row) for row in files_by_format],
            "recent_activity": [dict(row) for row in recent_activity]
        }

    except Exception as e:
        logger.exception("Metrics query failed")
        raise HTTPException(status_code=500, detail="An error occurred while fetching metrics.")
    finally:
        cursor.close()
        conn.close()

# ==================== FILE MANAGEMENT ====================

@app.get("/api/files", tags=["Files"])
async def list_files(
    current_user: Annotated[User, Depends(get_current_user)],
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    format: Optional[str] = None,
    search: Optional[str] = None
):
    """List files with RBAC (Users only see their own files, Admins see all)"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        where_clauses = ["1=1"]
        params = []
        
        # RBAC: regular users only see their own files
        if current_user.role != "admin":
            where_clauses.append("c.uploaded_by = %s")
            params.append(current_user.id)
        
        if format:
            where_clauses.append("c.file_format = %s")
            params.append(format)
        
        if search:
            where_clauses.append("c.object_name ILIKE %s")
            params.append(f"%{search}%")
        
        where_sql = " AND ".join(where_clauses)
        
        # Count total
        cursor.execute(f"SELECT COUNT(*) as total FROM minio_data_catalog c WHERE {where_sql}", params)
        total = cursor.fetchone()['total']
        
        # Get files with uploader username
        cursor.execute(f"""
            SELECT 
                c.catalog_id, c.bucket_name, c.object_name, c.object_size,
                c.file_format, c.created_at, c.metadata,
                u.username as uploaded_by_user
            FROM minio_data_catalog c
            LEFT JOIN users u ON c.uploaded_by = u.id
            WHERE {where_sql}
            ORDER BY c.created_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        
        files = cursor.fetchall()
        return {"total": total, "files": files, "limit": limit, "offset": offset}
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
        
@app.delete("/api/files/{catalog_id}", tags=["Files"])
async def delete_file(catalog_id: int, current_user: Annotated[User, Depends(get_current_user)]):
    """Delete a file from both catalog and MinIO"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Get file info
        cursor.execute("SELECT bucket_name, object_name, uploaded_by FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
        
        # RBAC check
        if current_user.role != 'admin' and file_info['uploaded_by'] != current_user.id:
            raise HTTPException(status_code=403, detail="Not authorized to delete this file")

        # Delete from MinIO
        try:
            minio_client.remove_object(file_info['bucket_name'], file_info['object_name'])
        except Exception as e:
            logger.error(f"MinIO delete failed: {e}")
        
        # Delete from catalog
        cursor.execute("DELETE FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        conn.commit()
        log_audit(current_user.id, "DELETE_FILE", f"Deleted: {file_info['object_name']}")
        
        return {"status": "deleted", "catalog_id": catalog_id}
        
    finally:
        cursor.close()
        conn.close()

@app.get("/api/files/download/{catalog_id}", tags=["Files"])
async def download_file(catalog_id: int, current_user: Annotated[User, Depends(get_current_user)]):
    """Download a file from MinIO with authorization check"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT bucket_name, object_name, uploaded_by FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
            
        # RBAC check
        if current_user.role != 'admin' and file_info['uploaded_by'] != current_user.id:
            raise HTTPException(status_code=403, detail="Not authorized to download this file")
            
        try:
            response = minio_client.get_object(file_info['bucket_name'], file_info['object_name'])
            # Stream the response
            from fastapi.responses import StreamingResponse
            return StreamingResponse(
                response,
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={file_info['object_name'].split('/')[-1]}"}
            )
        except Exception as e:
            logger.error(f"MinIO download failed: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve file from storage")
            
    finally:
        cursor.close()
        conn.close()

# ==================== FILE UPLOAD ====================

@app.post("/api/upload", tags=["Files"])
async def upload_file(
    current_user: Annotated[User, Depends(get_current_user)],
    file: UploadFile = File(...)
):
    """Enterprise Upload with MIME detection and Pipeline Triggering"""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    filename = secure_filename(file.filename)
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    
    if ext not in ALLOWED_EXT:
        raise HTTPException(status_code=400, detail=f"File type .{ext} not supported")
    
    # MIME detection using magic
    content = await file.read()
    mime_type = magic.from_buffer(content, mime=True)
    
    file_type_map = {
        'csv': 'structured', 'json': 'structured', 'parquet': 'structured',
        'pdf': 'pdf', 'docx': 'docx', 'doc': 'docx',
        'png': 'image', 'jpg': 'image', 'jpeg': 'image'
    }
    file_type = file_type_map.get(ext, 'unstructured')
    object_name = f"raw/{file_type}/{filename}"
    
    try:
        # Upload to MinIO
        minio_client.put_object(
            BUCKET,
            object_name,
            io.BytesIO(content),
            length=len(content),
            content_type=mime_type or file.content_type
        )
        
        # Prepare metadata
        metadata = {
            "original_filename": file.filename,
            "mime_type": mime_type,
            "uploaded_by_username": current_user.username,
            "upload_timestamp": datetime.utcnow().isoformat(),
            "enterprise_tier": "gold"
        }
        
        # Update Catalog
        update_catalog(
            bucket=BUCKET,
            object_name=object_name,
            object_size=len(content),
            file_format=ext,
            uploaded_by=current_user.id,
            metadata=metadata
        )
        
        log_audit(current_user.id, "UPLOAD", f"Uploaded file: {object_name}")
        
        return {
            "status": "success",
            "object_name": object_name,
            "metadata": metadata,
            "message": "Data ingested. ETL pipeline automatically queued."
        }
    except Exception as e:
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/api/jobs", tags=["Jobs"])
async def list_jobs(current_user: Annotated[User, Depends(get_current_user)]):
    """Fetch ETL job status from Airflow API"""
    try:
        # Fetch DAGs
        dags_resp = requests.get(
            f"{AIRFLOW_URL}/dags",
            auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5
        )
        dags_resp.raise_for_status()
        dags = dags_resp.json().get('dags', [])
        
        # Fetch recent runs for the first few DAGs
        job_list = []
        for dag in dags[:10]:  # Limit for performance
            dag_id = dag['dag_id']
            runs_resp = requests.get(
                f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
                auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=2
            )
            runs = runs_resp.json().get('dag_runs', []) if runs_resp.status_code == 200 else []
            
            job_list.append({
                "id": dag_id,
                "label": dag.get('description') or dag_id,
                "status": runs[0]['state'] if runs else "never_run",
                "last_run": runs[0]['execution_date'] if runs else None,
                "is_paused": dag.get('is_paused', False)
            })
            
        return {"jobs": job_list}
    except Exception as e:
        logger.error(f"Airflow API unreachable: {e}")
        return {"jobs": [], "warning": "Airflow service unreachable"}

@app.post("/api/jobs/trigger/{dag_id}", tags=["Jobs"])
async def trigger_job(dag_id: str, current_user: Annotated[User, Depends(get_current_user)]):
    """Trigger an Airflow DAG run"""
    try:
        resp = requests.post(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns",
            auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS),
            json={},  # Empty conf
            timeout=5
        )
        resp.raise_for_status()
        log_audit(current_user.id, "TRIGGER_JOB", f"Triggered DAG: {dag_id}")
        return {"status": "success", "data": resp.json()}
    except Exception as e:
        logger.error(f"Failed to trigger DAG {dag_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger pipeline: {str(e)}")

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
        
        return results
        
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

# ==================== USER MANAGEMENT (ADMIN ONLY) ====================

@app.get("/api/admin/users", response_model=List[User])
async def list_users(admin: Annotated[User, Depends(admin_only)]):
    """List all users in the system"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, username, role FROM users ORDER BY id ASC")
        users = cursor.fetchall()
        return [User(**u) for u in users]
    finally:
        cursor.close()
        conn.close()

class CreateUser(BaseModel):
    username: str
    password: str
    role: str = "user"

@app.post("/api/admin/users")
async def create_user(payload: CreateUser, admin: Annotated[User, Depends(admin_only)]):
    """Create a new user"""
    hashed_password = pwd_context.hash(payload.password)
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
            (payload.username, hashed_password, payload.role)
        )
        conn.commit()
        return {"status": "User created successfully"}
    except psycopg2.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=409, detail="Username already taken")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create user: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.delete("/api/admin/users/{user_id}")
async def delete_user(user_id: int, admin: Annotated[User, Depends(admin_only)]):
    """Delete a user"""
    if admin.id == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
        
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found")
        conn.commit()
        log_audit(admin.id, "DELETE_USER", f"Deleted user ID: {user_id}")
        return {"status": "User deleted"}
    finally:
        cursor.close()
        conn.close()

@app.get("/api/admin/audit-logs", tags=["Admin"])
async def get_audit_logs(admin: Annotated[User, Depends(admin_only)], limit: int = 50):
    """Fetch recent system audit logs"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT a.id, a.action, a.details, a.ip_address, a.created_at, u.username
            FROM audit_logs a
            LEFT JOIN users u ON a.user_id = u.id
            ORDER BY a.created_at DESC
            LIMIT %s
        """, (limit,))
        logs = cursor.fetchall()
        return logs
    finally:
        cursor.close()
        conn.close()


# ==================== HEALTH CHECK ====================

@app.get("/api/health", tags=["Health"])
async def health_check():
    """Check health of all services"""
    health = {
        "status": "healthy",
        "api": "ok",
        "postgres": "unknown",
        "minio": "unknown",
        "airflow": "unknown",
        "timestamp": datetime.utcnow().isoformat()
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
        health["postgres"] = f"error"
        health["status"] = "degraded"
    
    # Check MinIO
    try:
        minio_client.bucket_exists(BUCKET)
        health["minio"] = "ok"
    except Exception as e:
        health["minio"] = f"error"
        health["status"] = "degraded"

    # Check Airflow
    try:
        resp = requests.get(f"{AIRFLOW_URL}/health", timeout=2)
        if resp.status_code == 200:
            health["airflow"] = "ok"
        else:
            health["airflow"] = "degraded"
    except:
        health["airflow"] = "offline"
    
    return health

@app.get("/api/admin/diagnostics", tags=["Admin"])
async def get_diagnostics(admin: Annotated[User, Depends(admin_only)]):
    """System diagnostics for the console"""
    import platform
    import psutil
    
    return {
        "os": platform.system(),
        "processor": platform.processor(),
        "cpu_usage": psutil.cpu_percent(),
        "memory": psutil.virtual_memory()._asdict(),
        "disk": psutil.disk_usage('/')._asdict(),
        "python_version": platform.python_version(),
        "active_threads": psutil.Process().num_threads(),
        "open_files": len(psutil.Process().open_files()),
        "network": psutil.net_io_counters()._asdict()
    }

@app.get("/api/admin/system/stats", tags=["Admin"])
async def get_system_stats(admin: Annotated[User, Depends(admin_only)]):
    """Detailed system stats for Admin only"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) as count FROM users")
        user_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM minio_data_catalog")
        file_count = cursor.fetchone()['count']
        
        return {
            "total_users": user_count,
            "total_documents": file_count,
            "api_version": "2.0.0-enterprise",
            "environment": os.getenv('NODE_ENV', 'production')
        }
    finally:
        cursor.close()
        conn.close()

def update_catalog(bucket, object_name, object_size=None, file_format=None, uploaded_by=None, metadata: dict = None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        # Schema migration/check included in update
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minio_data_catalog (
                catalog_id SERIAL PRIMARY KEY,
                bucket_name TEXT NOT NULL,
                object_name TEXT NOT NULL,
                object_size BIGINT,
                file_format TEXT,
                row_count INTEGER,
                text_extracted BOOLEAN DEFAULT FALSE,
                metadata JSONB DEFAULT '{}'::JSONB,
                last_modified TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uploaded_by INTEGER REFERENCES users(id),
                UNIQUE(bucket_name, object_name)
            )
        """)
        # Ensure metadata column exists (for older versions)
        cursor.execute("ALTER TABLE minio_data_catalog ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::JSONB")
        
        cursor.execute("""
            INSERT INTO minio_data_catalog (bucket_name, object_name, object_size, file_format, uploaded_by, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (bucket_name, object_name) DO UPDATE
            SET object_size = EXCLUDED.object_size,
                file_format = EXCLUDED.file_format,
                uploaded_by = EXCLUDED.uploaded_by,
                metadata = EXCLUDED.metadata,
                last_modified = CURRENT_TIMESTAMP
        """, (bucket, object_name, object_size, file_format, uploaded_by, json.dumps(metadata or {})))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Catalog update failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Catalog update failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
