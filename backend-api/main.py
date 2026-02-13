from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Optional, Annotated
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from datetime import datetime, timedelta, date
import os
import io
from werkzeug.utils import secure_filename
from passlib.context import CryptContext
from jose import JWTError, jwt
import json

from ldap3 import Server, Connection, ALL, SIMPLE, ALL_ATTRIBUTES

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

LDAP_SERVER_URI = "ldap://host.docker.internal:389"
LDAP_BASE_DN = "dc=example,dc=com"
LDAP_USERS_OU = "ou=users," + LDAP_BASE_DN

# Your gidNumber mapping
LDAP_ADMIN_GID = "501"
LDAP_USER_GID = "500"

app = FastAPI(title="Lakehouse Admin API")

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for DEV MODE ONLY !! must be changed when in prod !!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_or_create_internal_user(db, username: str):
    cursor = db.cursor()

    cursor.execute(
        "SELECT id FROM users WHERE username = %s",
        (username,)
    )
    row = cursor.fetchone()

    if row:
        return row["id"]

    cursor.execute(
        """
        INSERT INTO users (username)
        VALUES (%s)
        RETURNING id
        """,
        (username,)
    )
    user_id = cursor.fetchone()["id"]
    db.commit()
    return user_id


# ldap authentication -- isko please touch mat karna meri kasam hai tumhe!
def ldap_authenticate(username: str, password: str):
    """
    Authenticates a user against LDAP using bind.
    Returns: dict { username, role }
    Raises: HTTPException on failure
    """

    user_dn = f"cn={username},{LDAP_USERS_OU}"

    server = Server(LDAP_SERVER_URI, get_info=ALL)

    try:
        # Bind as the user (this validates password)
        conn = Connection(
            server,
            user=user_dn,
            password=password,
            authentication=SIMPLE,
            auto_bind=True
        )

        # Fetch user attributes
        conn.search(
            search_base=user_dn,
            search_filter="(objectClass=posixAccount)",
            attributes=ALL_ATTRIBUTES
        )

        if not conn.entries:
            raise HTTPException(status_code=401, detail="User not found in LDAP")

        entry = conn.entries[0]
        gid_number = str(entry.gidNumber.value)

        # Map role
        if gid_number == LDAP_ADMIN_GID:
            role = "admin"
        else:
            role = "user"

        conn.unbind()

        return {
            "username": username,
            "role": role
        }

    except Exception:
        raise HTTPException(status_code=401, detail="Invalid LDAP credentials")

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

SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-super-secret-key-here')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Pydantic Models
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

class CurrentUser(BaseModel):
    id: int
    username: str
    role: str

# Helper to get user from DB
# def get_user_from_db(username: str):
#     conn = get_db()
#     cursor = conn.cursor()
#     try:
#         cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username = %s", (username,))
#         return cursor.fetchone()
#     finally:
#         cursor.close()
#         conn.close()

# Authenticate user
# def authenticate_user(username: str, password: str):
#     user = get_user_from_db(username)
#     if not user or not pwd_context.verify(password, user['password_hash']):
#         return False
#     return user

# Create JWT token
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Get current user from token
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")

        if username is None or role is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    db = get_db()

    internal_user = get_or_create_internal_user(db, username)

    return CurrentUser(
        id=internal_user,
        username=username,
        role=role,
    )

# Role Checker
class RoleChecker:
    def __init__(self, allowed_roles: List[str]):
        self.allowed_roles = allowed_roles

    def __call__(self, user: Annotated[User, Depends(get_current_user)]):
        if user.role not in self.allowed_roles:
            raise HTTPException(status_code=403, detail="Operation not permitted")
        return user

admin_only = RoleChecker(["admin"])

# ==================== AUTH ENDPOINTS ====================

# @app.post("/api/auth/login", response_model=Token)
# async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
#     user = authenticate_user(form_data.username, form_data.password)
#     if not user:
#         raise HTTPException(status_code=401, detail="Incorrect username or password")
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     access_token = create_access_token(
#         data={"sub": user['username'], "id": user['id'], "role": user['role']},
#         expires_delta=access_token_expires
#     )
#     return {"access_token": access_token}

@app.post("/api/auth/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    ldap_user = ldap_authenticate(
        form_data.username,
        form_data.password
    )

    db = get_db()
    internal_user_id = get_or_create_internal_user(db, ldap_user["username"])

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={
            "sub": ldap_user["username"],
            "user_id": internal_user_id,
            "role": ldap_user["role"]
        },
        expires_delta=access_token_expires
    )

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }


# @app.post("/api/auth/change-password")
# async def change_password(
#     payload: ChangePassword,
#     current_user: Annotated[User, Depends(get_current_user)]
# ):
#     """Change the current user's password"""
#     conn = get_db()
#     cursor = conn.cursor()
#     try:
#         cursor.execute("SELECT password_hash FROM users WHERE id = %s", (current_user.id,))
#         user = cursor.fetchone()
#         if not user:
#             raise HTTPException(status_code=404, detail="User not found")
        
#         if not pwd_context.verify(payload.old_password, user['password_hash']):
#             raise HTTPException(status_code=400, detail="Incorrect old password")
        
#         new_hash = pwd_context.hash(payload.new_password)
#         cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (new_hash, current_user.id))
#         conn.commit()
        
#         return {"status": "Password changed successfully"}
    
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=500, detail=f"Password change failed: {str(e)}")
#     finally:
#         cursor.close()
#         conn.close()

# @app.post("/api/auth/register")
# async def register_user(user: RegisterUser):
#     hashed_password = pwd_context.hash(user.password)
#     conn = get_db()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(
#             "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, 'user')",
#             (user.username, hashed_password)
#         )
#         conn.commit()
#         return {"status": "User created successfully"}
#     except psycopg2.IntegrityError:
#         conn.rollback()
#         raise HTTPException(status_code=409, detail="Username already taken")
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=500, detail="Registration failed")
#     finally:
#         cursor.close()
#         conn.close()

# ==================== DASHBOARD METRICS ====================

@app.get("/api/dashboard/metrics")
def get_dashboard_metrics(current_user: User = Depends(get_current_user)):
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        metrics = {}
        is_admin = current_user.role == 'admin'

        # -------------------------------------------------
        # Base user filter (org paradigm)
        # -------------------------------------------------
        user_clause = ""
        user_params = []
        if not is_admin:
            user_clause = " AND uploaded_by = %s"
            user_params = [current_user.id]

        # -------------------------------------------------
        # Total documents
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT COUNT(*) AS total_documents
            FROM minio_data_catalog
            WHERE 1=1 {user_clause}
            """,
            user_params
        )
        metrics["total_documents"] = cursor.fetchone()["total_documents"]

        # Total users (for admin)
        if is_admin:
            cursor.execute("SELECT COUNT(*) as count FROM users WHERE role != 'admin'")
            metrics["total_users"] = cursor.fetchone()["count"]
        else:
            metrics["total_users"] = 0

        # -------------------------------------------------
        # Processed today
        # -------------------------------------------------
        today = date.today()
        cursor.execute(
            f"""
            SELECT COUNT(*) AS processed_today
            FROM minio_data_catalog
            WHERE text_extracted = TRUE
              AND DATE(created_at) = %s
              {user_clause}
            """,
            [today] + user_params
        )
        metrics["processed_today"] = cursor.fetchone()["processed_today"]

        # -------------------------------------------------
        # Raw files count (raw/)
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT COUNT(*) AS raw_documents
            FROM minio_data_catalog
            WHERE object_name LIKE 'raw/%'
              {user_clause}
            """,
            user_params
        )
        metrics["files_in_raw"] = cursor.fetchone()["raw_documents"]

        # -------------------------------------------------
        # Total storage used
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT COALESCE(SUM(object_size), 0) AS total_size
            FROM minio_data_catalog
            WHERE 1=1 {user_clause}
            """,
            user_params
        )
        total_bytes = cursor.fetchone()["total_size"] or 0

        if total_bytes >= 1024 ** 3:
            metrics["total_storage"] = f"{round(total_bytes / (1024 ** 3), 2)} GB"
        else:
            metrics["total_storage"] = f"{round(total_bytes / (1024 ** 2), 2)} MB"

        # -------------------------------------------------
        # Files by format
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT file_format, COUNT(*) AS count
            FROM minio_data_catalog
            WHERE file_format IS NOT NULL
              {user_clause}
            GROUP BY file_format
            ORDER BY count DESC
            """,
            user_params
        )
        metrics["files_by_format"] = cursor.fetchall()

        # -------------------------------------------------
        # Processing trend (last 7 days)
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT
                DATE(created_at) AS date,
                COUNT(*) AS count
            FROM minio_data_catalog
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
              {user_clause}
            GROUP BY DATE(created_at)
            ORDER BY date
            """,
            user_params
        )
        metrics["processing_trend"] = cursor.fetchall()

        # -------------------------------------------------
        # Recent activity
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT
                object_name,
                file_format,
                created_at,
                object_size,
                text_extracted
            FROM minio_data_catalog
            WHERE 1=1 {user_clause}
            ORDER BY created_at DESC
            LIMIT 10
            """,
            user_params
        )
        metrics["recent_activity"] = cursor.fetchall()

        # -------------------------------------------------
        # Extraction rate
        # -------------------------------------------------
        cursor.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE text_extracted = TRUE) AS extracted,
                COUNT(*) AS total
            FROM minio_data_catalog
            WHERE 1=1 {user_clause}
            """,
            user_params
        )
        stats = cursor.fetchone()
        metrics["extraction_rate"] = (
            round((stats["extracted"] / stats["total"]) * 100, 2)
            if stats["total"] > 0 else 0
        )

        return metrics

    except Exception as e:
        logger.error(f"Metrics query failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch metrics")

    finally:
        cursor.close()
        conn.close()

# ==================== FILE MANAGEMENT ====================

@app.get("/api/files")
async def list_files(
    current_user: Annotated[User, Depends(get_current_user)],
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None
):
    """List files with proper RBAC filtering"""
    logger.info(f"User {current_user.username} (id: {current_user.id}, role: {current_user.role}) requesting files")

    conn = get_db()
    cursor = conn.cursor()
    
    try:
        where_clauses = []
        params = []
        
        # RBAC: Non-admin users only see their own files
        if current_user.role != "admin":
            where_clauses.append("uploaded_by = %s")
            params.append(current_user.id)
        
        # Search filter
        if search:
            where_clauses.append("object_name ILIKE %s")
            params.append(f"%{search}%")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        logger.info(f"Query WHERE: {where_sql}, params: {params}")
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM minio_data_catalog WHERE {where_sql}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()['total']
        logger.info(f"Total files found: {total}")
        
        # Get paginated results
        files_query = f"""
            SELECT 
                catalog_id,
                bucket_name,
                object_name,
                object_size,
                file_format,
                row_count,
                text_extracted,
                content_hash,
                created_at,
                last_modified,
                uploaded_by,
                metadata
            FROM minio_data_catalog
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(files_query, params + [limit, offset])
        
        files = cursor.fetchall()
        logger.info(f"Fetched {len(files)} files")
        
        return {
            "total": total,
            "files": [dict(file) for file in files],
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.exception("List files failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/api/files/{catalog_id}")
async def get_file_details(
    catalog_id: int,
    current_user: Annotated[User, Depends(get_current_user)]
):
    """Get detailed information about a specific file"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute("SELECT * FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        else:
            cursor.execute(
                "SELECT * FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.username)
            )
        
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found or access denied")
        
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
            "catalog": dict(file_info),
            "minio_exists": minio_exists,
            "minio_info": minio_info
        }
        
    finally:
        cursor.close()
        conn.close()

@app.delete("/api/files/{catalog_id}")
async def delete_file(
    catalog_id: int,
    current_user: Annotated[User, Depends(get_current_user)]
):
    """Delete a file from both catalog and MinIO"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute(
                "SELECT bucket_name, object_name FROM minio_data_catalog WHERE catalog_id = %s",
                (catalog_id,)
            )
        else:
            cursor.execute(
                "SELECT bucket_name, object_name FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.id)
            )
        
        file_info = cursor.fetchone()
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found or access denied")
        
        # Delete from MinIO
        try:
            minio_client.remove_object(file_info['bucket_name'], file_info['object_name'])
            logger.info(f"Deleted from MinIO: {file_info['object_name']}")
        except Exception as e:
            logger.warning(f"MinIO delete failed: {e}")
        
        # Delete from catalog
        cursor.execute("DELETE FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        conn.commit()
        
        return {"status": "deleted", "catalog_id": catalog_id}
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# ==================== FILE UPLOAD ====================



@app.get("/api/admin/audit-logs")
def get_audit_logs(current_user: User = Depends(get_current_user)):
    """Get system audit logs"""
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Admin privileges required")
    
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Synthesize audit logs from catalog activity for now
        cursor.execute("""
            SELECT 
                catalog_id as id,
                'UPLOAD' as action,
                object_name as details,
                created_at,
                uploaded_by as user_id,
                (SELECT username FROM users WHERE id = minio_data_catalog.uploaded_by) as username
            FROM minio_data_catalog
            ORDER BY created_at DESC
            LIMIT 50
        """)
        logs = cursor.fetchall()
        return logs
    finally:
        cursor.close()
        conn.close()

# ==================== FILE UPLOAD ====================

@app.post("/api/upload")
async def upload_files(
    current_user: Annotated[User, Depends(get_current_user)],
    files: List[UploadFile] = File(...)
):
    """
    Upload multiple files to MinIO raw bucket
    and persist uploader identity via object metadata.
    """

    uploaded_files = []
    errors = []

    for file in files:
        if not file.filename:
            continue

        # ---- Validate extension ----
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        if ext not in ALLOWED_EXT:
            errors.append(f"{file.filename}: .{ext} not supported")
            continue

        filename = secure_filename(file.filename)

        file_type_map = {
            "csv": "structured", "json": "structured", "parquet": "structured",
            "pdf": "pdf",
            "doc": "docx", "docx": "docx",
            "png": "image", "jpg": "image", "jpeg": "image", "tiff": "image",
            "ppt": "ppt", "pptx": "ppt",
        }

        file_type = file_type_map.get(ext, "other")
        object_name = f"raw/{filename}"

        try:
            content = await file.read()

            # ---- CRITICAL FIX: store uploader identity in MinIO metadata ----
            minio_client.put_object(
                BUCKET,
                object_name,
                io.BytesIO(content),
                length=len(content),
                content_type=file.content_type or "application/octet-stream",
                metadata={
                    "uploaded-by": str(current_user.id),
                    "uploaded-by-username": current_user.username,
                    "source": "api_upload",
                },
            )

            # ---- Catalog entry for RAW file ----
            update_catalog(
                bucket=BUCKET,
                object_name=object_name,
                object_size=len(content),
                file_format=file_type,
                uploaded_by=current_user.id,
                metadata={
                    "original_filename": file.filename,
                    "mime_type": file.content_type,
                    "upload_time": datetime.utcnow().isoformat(),
                    "source": "api_upload",
                },
            )

            uploaded_files.append(object_name)
            logger.info(
                f"User {current_user.username} ({current_user.id}) uploaded {object_name}"
            )

        except Exception as e:
            logger.exception(f"Upload failed for {filename}")
            errors.append(f"{filename}: {str(e)}")

    if not uploaded_files:
        raise HTTPException(
            status_code=500,
            detail={"message": "Upload failed", "errors": errors},
        )

    return {
        "status": "success",
        "uploaded": uploaded_files,
        "errors": errors,
        "message": f"Uploaded {len(uploaded_files)} files",
    }

# ==================== SEARCH ====================

@app.get("/api/search")
async def search_documents(
    query: str,
    current_user: Annotated[User, Depends(get_current_user)],
    limit: int = 20
):
    """Search across extracted text in unstructured_documents"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # RBAC: Admin sees all, users see only their documents
        if current_user.role == 'admin':
            search_query = """
                SELECT 
                    ud.id,
                    ud.object_name,
                    ud.file_type,
                    LEFT(ud.text_content, 200) as preview,
                    ud.created_at,
                    mdc.uploaded_by
                FROM unstructured_documents ud
                LEFT JOIN minio_data_catalog mdc ON ud.object_name = mdc.object_name
                WHERE ud.text_content ILIKE %s
                ORDER BY ud.created_at DESC
                LIMIT %s
            """
            cursor.execute(search_query, (f"%{query}%", limit))
        else:
            search_query = """
                SELECT 
                    ud.id,
                    ud.object_name,
                    ud.file_type,
                    LEFT(ud.text_content, 200) as preview,
                    ud.created_at,
                    mdc.uploaded_by
                FROM unstructured_documents ud
                INNER JOIN minio_data_catalog mdc ON ud.object_name = mdc.object_name
                WHERE ud.text_content ILIKE %s AND mdc.uploaded_by = %s
                ORDER BY ud.created_at DESC
                LIMIT %s
            """
            cursor.execute(search_query, (f"%{query}%", current_user.id, limit))
        
        results = cursor.fetchall()
        
        return {
            "query": query,
            "count": len(results),
            "results": [dict(row) for row in results]
        }
        
    finally:
        cursor.close()
        conn.close()

# ==================== STATS ====================

@app.get("/api/stats/storage")
async def get_storage_stats(current_user: Annotated[User, Depends(get_current_user)]):
    """Get storage statistics by file type"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if current_user.role == 'admin':
            stats_query = """
                SELECT 
                    file_format,
                    COUNT(*) as file_count,
                    SUM(object_size) as total_size,
                    AVG(object_size) as avg_size
                FROM minio_data_catalog
                WHERE file_format IS NOT NULL
                GROUP BY file_format
                ORDER BY total_size DESC
            """
            cursor.execute(stats_query)
        else:
            stats_query = """
                SELECT 
                    file_format,
                    COUNT(*) as file_count,
                    SUM(object_size) as total_size,
                    AVG(object_size) as avg_size
                FROM minio_data_catalog
                WHERE file_format IS NOT NULL AND uploaded_by = %s
                GROUP BY file_format
                ORDER BY total_size DESC
            """
            cursor.execute(stats_query, [current_user.id])
        
        stats = cursor.fetchall()
        
        return {
            "storage_by_type": [
                {
                    **dict(row),
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
    """Get processing statistics and trends"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Last 30 days trend
        if current_user.role == 'admin':
            trend_query = """
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as count,
                    file_format
                FROM minio_data_catalog
                WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(created_at), file_format
                ORDER BY date DESC, file_format
            """
            cursor.execute(trend_query)
        else:
            trend_query = """
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as count,
                    file_format
                FROM minio_data_catalog
                WHERE uploaded_by = %s AND created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(created_at), file_format
                ORDER BY date DESC, file_format
            """
            cursor.execute(trend_query, [current_user.id])
        
        trend = cursor.fetchall()
        
        # Text extraction success rate
        if current_user.role == 'admin':
            extraction_query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN text_extracted THEN 1 ELSE 0 END) as extracted
                FROM minio_data_catalog
                WHERE file_format IN ('pdf', 'docx', 'pptx')
            """
            cursor.execute(extraction_query)
        else:
            extraction_query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN text_extracted THEN 1 ELSE 0 END) as extracted
                FROM minio_data_catalog
                WHERE file_format IN ('pdf', 'docx', 'pptx') AND uploaded_by = %s
            """
            cursor.execute(extraction_query, [current_user.id])
        
        extraction_stats = cursor.fetchone()
        
        return {
            "daily_trend": [dict(row) for row in trend],
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

def update_catalog(bucket, object_name, object_size=None, file_format=None, uploaded_by=None, metadata=None):
    """Update catalog with file information"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        # Ensure table exists with metadata column
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minio_data_catalog (
                catalog_id SERIAL PRIMARY KEY,
                bucket_name TEXT NOT NULL,
                object_name TEXT NOT NULL,
                object_size BIGINT,
                file_format TEXT,
                row_count INTEGER,
                text_extracted BOOLEAN DEFAULT FALSE,
                content_hash TEXT,
                last_modified TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uploaded_by INTEGER,
                metadata JSONB,
                UNIQUE(bucket_name, object_name)
            )
        """)
        
        # Insert or update
        metadata_json = json.dumps(metadata) if metadata else None
        
        cursor.execute("""
            INSERT INTO minio_data_catalog 
                (bucket_name, object_name, object_size, file_format, uploaded_by, metadata)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (bucket_name, object_name) DO UPDATE
            SET object_size = EXCLUDED.object_size,
                file_format = EXCLUDED.file_format,
                uploaded_by = EXCLUDED.uploaded_by,
                metadata = EXCLUDED.metadata,
                last_modified = CURRENT_TIMESTAMP
        """, (bucket, object_name, object_size, file_format, uploaded_by, metadata_json))
        
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
    