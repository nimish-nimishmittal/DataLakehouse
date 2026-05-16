from fastapi import FastAPI, HTTPException, UploadFile, File, Depends, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel
from typing import List, Optional, Annotated
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from datetime import datetime, timedelta, date
import os
import io
import csv
import mimetypes
from werkzeug.utils import secure_filename
from passlib.context import CryptContext
from jose import JWTError, jwt
import json
import requests
from requests.auth import HTTPBasicAuth
from ldap3 import Server, Connection, ALL, SIMPLE, ALL_ATTRIBUTES
import logging

# Document processing imports
try:
    from pypdf import PdfReader
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

try:
    import docx
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False

try:
    import pptx
    PPTX_AVAILABLE = True
except ImportError:
    PPTX_AVAILABLE = False

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

LDAP_SERVER_URI = "ldap://host.docker.internal:389"
LDAP_BASE_DN = "dc=example,dc=com"
LDAP_USERS_OU = "ou=users," + LDAP_BASE_DN

# Your gidNumber mapping
LDAP_ADMIN_GID = "501"
LDAP_USER_GID = "500"

AIRFLOW_URL = "http://airflow:8080/api/v1"

# AIRFLOW_URL = os.getenv('AIRFLOW_URL', 'http://airflow:8080/')
AIRFLOW_USER = os.getenv('AIRFLOW_USER', 'admin')
AIRFLOW_PASS = os.getenv('AIRFLOW_PASS', 'admin')

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

def get_or_create_internal_user(db, username: str):
    cursor = db.cursor(cursor_factory=RealDictCursor)

    cursor.execute(
        "SELECT id FROM users WHERE username = %s",
        (username,)
    )
    row = cursor.fetchone()

    if row:
        cursor.close()
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
    cursor.close()
    return user_id

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

# ldap authentication -- isko please touch mat karna meri kasam hai tumhe! RIP nimish 25/2/26
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
    'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp', 'ico', 'tiff', 'avif',
    'pptx', 'ppt'
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
    log_audit(internal_user_id, "LOGIN", f"User {ldap_user['username']} logged in")
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

        # ====================== TOTAL DOCUMENTS ======================
        if is_admin:
            cursor.execute("SELECT COUNT(*) AS total_documents FROM minio_data_catalog")
        else:
            cursor.execute(
                "SELECT COUNT(*) AS total_documents FROM minio_data_catalog WHERE uploaded_by = %s",
                (current_user.id,)
            )
        metrics["total_documents"] = cursor.fetchone()["total_documents"]

        # ====================== TOTAL USERS (admin only) ======================
        if is_admin:
            cursor.execute("SELECT COUNT(*) as count FROM users")
            metrics["total_users"] = cursor.fetchone()["count"]
        else:
            metrics["total_users"] = None

        # ====================== PROCESSED TODAY ======================
        today = date.today()
        if is_admin:
            cursor.execute(
                """
                SELECT COUNT(*) AS processed_today
                FROM minio_data_catalog
                WHERE text_extracted = TRUE AND DATE(created_at) = %s
                """,
                (today,)
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) AS processed_today
                FROM minio_data_catalog
                WHERE text_extracted = TRUE 
                  AND DATE(created_at) = %s 
                  AND uploaded_by = %s
                """,
                (today, current_user.id)
            )
        metrics["processed_today"] = cursor.fetchone()["processed_today"]

        # ====================== RAW FILES ======================
        if is_admin:
            cursor.execute(
                """
                SELECT COUNT(*) AS raw_documents
                FROM minio_data_catalog
                WHERE object_name LIKE 'raw/%'
                """
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) AS raw_documents
                FROM minio_data_catalog
                WHERE object_name LIKE 'raw/%' AND uploaded_by = %s
                """,
                (current_user.id,)
            )
        metrics["files_in_raw"] = cursor.fetchone()["raw_documents"]

        # ====================== TOTAL STORAGE ======================
        if is_admin:
            cursor.execute(
                "SELECT COALESCE(SUM(object_size), 0) AS total_size FROM minio_data_catalog"
            )
        else:
            cursor.execute(
                """
                SELECT COALESCE(SUM(object_size), 0) AS total_size 
                FROM minio_data_catalog 
                WHERE uploaded_by = %s
                """,
                (current_user.id,)
            )
        total_bytes = cursor.fetchone()["total_size"] or 0

        if total_bytes >= 1024 ** 3:
            metrics["total_storage"] = f"{round(total_bytes / (1024 ** 3), 2)} GB"
        else:
            metrics["total_storage"] = f"{round(total_bytes / (1024 ** 2), 2)} MB"

        # ====================== FILES BY FORMAT ======================
        if is_admin:
            cursor.execute(
                """
                SELECT file_format, COUNT(*) AS count
                FROM minio_data_catalog
                WHERE file_format IS NOT NULL
                GROUP BY file_format
                ORDER BY count DESC
                """
            )
        else:
            cursor.execute(
                """
                SELECT file_format, COUNT(*) AS count
                FROM minio_data_catalog
                WHERE file_format IS NOT NULL AND uploaded_by = %s
                GROUP BY file_format
                ORDER BY count DESC
                """,
                (current_user.id,)
            )
        metrics["files_by_format"] = [dict(r) for r in cursor.fetchall()]

        # ====================== PROCESSING TREND (last 7 days) ======================
        if is_admin:
            cursor.execute(
                """
                SELECT DATE(created_at) AS date, COUNT(*) AS count
                FROM minio_data_catalog
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date
                """
            )
        else:
            cursor.execute(
                """
                SELECT DATE(created_at) AS date, COUNT(*) AS count
                FROM minio_data_catalog
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' AND uploaded_by = %s
                GROUP BY DATE(created_at)
                ORDER BY date
                """,
                (current_user.id,)
            )
        metrics["processing_trend"] = [
            {**dict(r), "date": r["date"].isoformat() if r["date"] else None}
            for r in cursor.fetchall()
        ]

        # ====================== RECENT ACTIVITY ======================
        if is_admin:
            cursor.execute(
                """
                SELECT object_name, file_format, created_at, object_size, text_extracted
                FROM minio_data_catalog
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
        else:
            cursor.execute(
                """
                SELECT object_name, file_format, created_at, object_size, text_extracted
                FROM minio_data_catalog
                WHERE uploaded_by = %s
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (current_user.id,)
            )
        metrics["recent_activity"] = [
            {**dict(r), "created_at": r["created_at"].isoformat() if r["created_at"] else None}
            for r in cursor.fetchall()
        ]

        # ====================== EXTRACTION RATE ======================
        if is_admin:
            cursor.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE text_extracted = TRUE) AS extracted,
                    COUNT(*) AS total
                FROM minio_data_catalog
                """
            )
        else:
            cursor.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE text_extracted = TRUE) AS extracted,
                    COUNT(*) AS total
                FROM minio_data_catalog
                WHERE uploaded_by = %s
                """,
                (current_user.id,)
            )
        stats = cursor.fetchone()
        metrics["extraction_rate"] = (
            round((stats["extracted"] / stats["total"]) * 100, 2)
            if stats["total"] > 0 else 0
        )

        return metrics

    except Exception as e:
        logger.error(f"Metrics query failed: {str(e)}", exc_info=True)  # ← this now shows full traceback
        raise HTTPException(status_code=500, detail="Failed to fetch metrics")
    finally:
        cursor.close()
        conn.close()

# ==================== FILE BROWSER API ====================

from typing import Dict, List, Any

@app.get("/api/buckets")
async def list_buckets(
    current_user: Annotated[User, Depends(get_current_user)]
):
    """
    List all buckets where the user has at least one file.
    RBAC: Users only see buckets containing their uploaded files.
    Admins see all buckets that have any files in the catalog.
    """
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if current_user.role == "admin":
            query = """
                SELECT
                    bucket_name,
                    COUNT(*) as file_count,
                    SUM(object_size) as total_size,
                    MIN(created_at) as earliest_upload,
                    MAX(created_at) as latest_upload
                FROM minio_data_catalog
                GROUP BY bucket_name
                ORDER BY bucket_name
            """
            cursor.execute(query)
        else:
            query = """
                SELECT
                    bucket_name,
                    COUNT(*) as file_count,
                    SUM(object_size) as total_size,
                    MIN(created_at) as earliest_upload,
                    MAX(created_at) as latest_upload
                FROM minio_data_catalog
                WHERE uploaded_by = %s
                GROUP BY bucket_name
                ORDER BY bucket_name
            """
            cursor.execute(query, (current_user.id,))

        buckets = cursor.fetchall()

        return {
            "buckets": [dict(bucket) for bucket in buckets]
        }

    except Exception as e:
        logger.exception("List buckets failed")
        raise HTTPException(status_code=500, detail=f"Failed to list buckets: {str(e)}")
    finally:
        cursor.close()
        conn.close()


def _parse_folder_structure(
    files: List[Dict],
    prefix: str
) -> Dict[str, Any]:
    """
    Parse flat file list into folder structure.

    Args:
        files: List of file dicts from database
        prefix: Current folder prefix (e.g., "raw/" or "processed/structured/")

    Returns:
        {
            "folders": [
                {
                    "name": "folder_name/",
                    "path": "raw/folder_name/",
                    "file_count": 5,
                    "total_size": 1024000,
                    "latest_modified": "2024-01-15T..."
                }
            ],
            "files": [
                {
                    "catalog_id": 1,
                    "name": "file.csv",
                    "object_name": "raw/file.csv",
                    ...
                }
            ]
        }
    """
    folders: Dict[str, Dict] = {}
    root_files: List[Dict] = []

    prefix_len = len(prefix) if prefix else 0

    for file in files:
        object_name = file["object_name"]

        # Remove the prefix to get relative path
        if prefix and not object_name.startswith(prefix):
            continue

        relative_path = object_name[prefix_len:]

        # Check if there are more path separators (folders)
        if "/" in relative_path:
            # Extract immediate folder name
            folder_name = relative_path.split("/")[0] + "/"
            folder_path = prefix + folder_name if prefix else folder_name

            if folder_name not in folders:
                folders[folder_name] = {
                    "name": folder_name,
                    "path": folder_path,
                    "file_count": 0,
                    "total_size": 0,
                    "latest_modified": file.get("last_modified") or file.get("created_at"),
                    "formats": set()
                }

            # Aggregate stats
            folders[folder_name]["file_count"] += 1
            folders[folder_name]["total_size"] += file.get("object_size") or 0
            folders[folder_name]["formats"].add(file.get("file_format") or "unknown")

            # Update latest modified
            file_time = file.get("last_modified") or file.get("created_at")
            if file_time and file_time > folders[folder_name]["latest_modified"]:
                folders[folder_name]["latest_modified"] = file_time
        else:
            # This is a file in the current folder
            root_files.append({
                **dict(file),
                "display_name": relative_path.split("/")[-1]
            })

    # Convert sets to lists for JSON serialization
    folder_list = []
    for folder in folders.values():
        folder_list.append({
            **folder,
            "formats": list(folder["formats"])
        })

    return {
        "folders": folder_list,
        "files": root_files
    }


@app.get("/api/files")
async def list_files(
    current_user: Annotated[User, Depends(get_current_user)],
    bucket: str = Query(..., description="Bucket name to browse"),
    prefix: Optional[str] = Query(None, description="Folder prefix path (e.g., 'raw/' or 'processed/')"),
    search: Optional[str] = Query(None, description="Search within current folder"),
    limit: int = Query(1000, description="Max items to return")
):
    """
    File browser API - returns folder structure + files for a given bucket and prefix.

    RBAC applied: Users only see their own files, admins see all.
    Returns aggregated folder statistics and file metadata.
    """
    logger.info(f"User {current_user.username} browsing bucket={bucket}, prefix={prefix}")

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Build WHERE clause
        where_clauses = ["bucket_name = %s"]
        params = [bucket]

        # RBAC: Non-admin users only see their own files
        if current_user.role != "admin":
            where_clauses.append("uploaded_by = %s")
            params.append(current_user.id)

        # Prefix filter - files starting with this prefix
        if prefix:
            where_clauses.append("object_name LIKE %s")
            params.append(f"{prefix}%")

        # Search filter within current folder
        if search:
            if prefix:
                # Search within the prefix path
                where_clauses.append("object_name ILIKE %s")
                params.append(f"%{search}%")
            else:
                where_clauses.append("object_name ILIKE %s")
                params.append(f"%{search}%")

        where_sql = " AND ".join(where_clauses)

        # Query all matching files
        files_query = f"""
            SELECT
                catalog_id,
                bucket_name,
                object_name,
                object_size,
                file_format,
                row_count,
                text_extracted,
                created_at,
                last_modified,
                uploaded_by,
                metadata
            FROM minio_data_catalog
            WHERE {where_sql}
            ORDER BY object_name
            LIMIT %s
        """
        params.append(limit)
        cursor.execute(files_query, params)

        files = cursor.fetchall()
        logger.info(f"Fetched {len(files)} files for bucket={bucket}, prefix={prefix}")

        # Parse into folder structure
        structure = _parse_folder_structure(
            [dict(f) for f in files],
            prefix or ""
        )

        # Get breadcrumb path
        breadcrumbs = [{"name": bucket, "path": "", "is_bucket": True}]
        if prefix:
            parts = prefix.rstrip("/").split("/")
            current_path = ""
            for part in parts:
                current_path += part + "/"
                breadcrumbs.append({
                    "name": part + "/",
                    "path": current_path,
                    "is_bucket": False
                })

        return {
            "bucket": bucket,
            "prefix": prefix or "",
            "breadcrumbs": breadcrumbs,
            "folders": structure["folders"],
            "files": structure["files"],
            "total_items": len(structure["folders"]) + len(structure["files"]),
            "is_root": not prefix
        }

    except Exception as e:
        logger.exception("File browser list failed")
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# Legacy endpoint for backward compatibility (flat table view)
@app.get("/api/files/flat")
async def list_files_flat(
    current_user: Annotated[User, Depends(get_current_user)],
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None
):
    """Legacy endpoint - List all files as flat table with RBAC filtering"""
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
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute("SELECT * FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        else:
            cursor.execute(
                "SELECT * FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.id)
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

        log_audit(current_user.id, "DELETE_FILE", f"Deleted: {file_info['object_name']}")
        
        return {"status": "deleted", "catalog_id": catalog_id}
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# ==================== FILE DOWNLOAD ====================
@app.get("/api/files/download/{catalog_id}")
async def download_file_endpoint(
    catalog_id: int,
    current_user: Annotated[User, Depends(get_current_user)]
):
    """Download a file from MinIO"""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute("SELECT * FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        else:
            cursor.execute(
                "SELECT * FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.id)
            )

        file_info = cursor.fetchone()

        if not file_info:
            raise HTTPException(status_code=404, detail="File not found or access denied")

        bucket_name = file_info['bucket_name']
        object_name = file_info['object_name']
        display_name = file_info['display_name'] or object_name.split('/')[-1]
        file_format = file_info['file_format'] or ''

        # Get file from MinIO
        try:
            response = minio_client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
        except Exception as e:
            logger.error(f"MinIO download failed: {e}")
            raise HTTPException(status_code=404, detail="File not found in MinIO")

        # Determine MIME type
        mime_map = {
            'csv': 'text/csv',
            'json': 'application/json',
            'parquet': 'application/parquet',
            'pdf': 'application/pdf',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'doc': 'application/msword',
            'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'ppt': 'application/vnd.ms-powerpoint',
            'txt': 'text/plain',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'tiff': 'image/tiff',
        }
        content_type = mime_map.get(file_format.lower(), 'application/octet-stream')

        # Stream the file
        return StreamingResponse(
            io.BytesIO(data),
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{display_name}"'
            }
        )

    finally:
        cursor.close()
        conn.close()

# ==================== FILE PREVIEW ====================
@app.get("/api/files/preview/{catalog_id}")
async def preview_file_endpoint(
    catalog_id: int,
    current_user: Annotated[User, Depends(get_current_user)]
):
    """Get preview content of a file (CSV, Parquet, JSON, PDF, TXT)"""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute("SELECT * FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        else:
            cursor.execute(
                "SELECT * FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.id)
            )

        file_info = cursor.fetchone()

        if not file_info:
            raise HTTPException(status_code=404, detail="File not found or access denied")

        bucket_name = file_info['bucket_name']
        object_name = file_info['object_name']
        file_format = (file_info['file_format'] or '').lower()

        # Get file from MinIO
        try:
            response = minio_client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
        except Exception as e:
            logger.error(f"MinIO read failed: {e}")
            raise HTTPException(status_code=404, detail="File not found in MinIO")

        # Parse based on format
        if file_format in ['csv', 'structured']:
            # Parse CSV (including 'structured' format)
            text_data = data.decode('utf-8')
            reader = csv.reader(io.StringIO(text_data))
            rows = list(reader)[:100]  # Limit to 100 rows
            if len(rows) > 0:
                columns = rows[0]
                data_rows = rows[1:]
            else:
                columns = []
                data_rows = []
            return {"columns": columns, "rows": data_rows, "format": "csv", "total_rows": len(data_rows)}

        elif file_format == 'parquet':
            # Parse Parquet
            if not PYARROW_AVAILABLE:
                raise HTTPException(status_code=503, detail="PyArrow not installed")
            try:
                table = pq.read_table(io.BytesIO(data))
                df_dict = table.to_pydict()
                # Convert to row format
                columns = list(df_dict.keys())
                num_rows = len(df_dict[columns[0]]) if columns else 0
                rows = []
                for i in range(min(num_rows, 100)):
                    row = {col: df_dict[col][i] for col in columns}
                    rows.append(row)
                return {"columns": columns, "rows": rows, "format": "parquet", "total_rows": num_rows}
            except Exception as e:
                logger.error(f"Parquet parse failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to parse parquet: {str(e)}")

        elif file_format == 'json':
            # Parse JSON
            try:
                text_data = data.decode('utf-8')
                json_data = json.loads(text_data)
                # Truncate if too large
                if len(text_data) > 100000:
                    json_data = json.loads(text_data[:100000] + '... [truncated]')
                return {"content": json_data, "format": "json"}
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

        elif file_format in ['txt', 'text']:
            # Plain text (including 'text' format)
            text_data = data.decode('utf-8')
            return {"text": text_data, "format": "txt"}

        elif file_format == 'pdf':
            # Extract text from PDF
            if not PYPDF_AVAILABLE:
                raise HTTPException(status_code=503, detail="pypdf not installed")
            try:
                pdf_reader = PdfReader(io.BytesIO(data))
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n\n"
                return {"text": text, "format": "pdf", "pages": len(pdf_reader.pages)}
            except Exception as e:
                logger.error(f"PDF extraction failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to extract PDF text: {str(e)}")

        elif file_format in ['docx', 'doc']:
            # Extract text from DOCX
            if not DOCX_AVAILABLE:
                raise HTTPException(status_code=503, detail="python-docx not installed")
            try:
                doc = docx.Document(io.BytesIO(data))
                text = "\n".join([para.text for para in doc.paragraphs])
                return {"text": text, "format": "docx"}
            except Exception as e:
                logger.error(f"DOCX extraction failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to extract DOCX text: {str(e)}")

        elif file_format in ['pptx', 'ppt']:
            # Extract text from PPTX
            if not PPTX_AVAILABLE:
                raise HTTPException(status_code=503, detail="python-pptx not installed")
            try:
                prs = pptx.Presentation(io.BytesIO(data))
                slides = []
                for i, slide in enumerate(prs.slides):
                    slide_text = ""
                    for shape in slide.shapes:
                        if hasattr(shape, "text"):
                            slide_text += shape.text + "\n"
                    slides.append({"slide_num": i + 1, "text": slide_text.strip()})
                return {"slides": slides, "format": "pptx", "total_slides": len(slides)}
            except Exception as e:
                logger.error(f"PPTX extraction failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to extract PPTX text: {str(e)}")

        elif file_format in ['image', 'png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp', 'ico', 'tiff', 'avif']:
            # Image preview — return base64 so frontend renders inline, no second request needed
            if not PIL_AVAILABLE:
                raise HTTPException(status_code=503, detail="Pillow not installed for image processing")
            try:
                import base64
                img = Image.open(io.BytesIO(data))
                width, height = img.size

                # Resolve the actual extension from the object name
                ext = object_name.rsplit('.', 1)[-1].lower() if '.' in object_name else 'jpeg'
                # Normalise: 'jpg' → 'jpeg' for MIME type
                mime_ext = 'jpeg' if ext == 'jpg' else ext
                response_format = mime_ext   # 'jpeg', 'png', 'gif', etc.

                # Thumbnail for large images so response stays reasonable
                MAX_DIM = 1600
                if width > MAX_DIM or height > MAX_DIM:
                    img.thumbnail((MAX_DIM, MAX_DIM), Image.LANCZOS)

                # Convert RGBA → RGB for JPEG (JPEG doesn't support alpha)
                if mime_ext == 'jpeg' and img.mode in ('RGBA', 'P', 'LA'):
                    img = img.convert('RGB')

                buf = io.BytesIO()
                pil_fmt = 'JPEG' if mime_ext == 'jpeg' else mime_ext.upper()
                img.save(buf, format=pil_fmt)
                b64 = base64.b64encode(buf.getvalue()).decode('utf-8')

                return {
                    "format":     "image",
                    "image_ext":  response_format,       # 'jpeg' | 'png' | 'gif' …
                    "mime_type":  f"image/{response_format}",
                    "base64":     b64,
                    "width":      width,
                    "height":     height,
                }
            except Exception as e:
                logger.error(f"Image processing failed: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to process image: {str(e)}")

        else:
            raise HTTPException(status_code=400, detail=f"Preview not supported for format: {file_format}")

    finally:
        cursor.close()
        conn.close()


async def get_current_user_optional(request: Request):
    """
    Get current user from Bearer token OR query param ?token=...
    Used by endpoints that need to work inside iframe/object viewers
    where the browser cannot set Authorization headers.
    Uses Request directly — avoids FastAPI Header() alias quirks.
    """
    token_value = None

    # 1. Try Authorization header (standard API calls)
    auth_header = request.headers.get("Authorization") or request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token_value = auth_header[7:].strip()

    # 2. Fall back to ?token= query param (embedded viewers, PDF iframe)
    if not token_value:
        token_value = request.query_params.get("token")

    if not token_value:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = jwt.decode(token_value, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")

        if username is None or role is None:
            logger.error(f"Token missing claims: username={username}, role={role}")
            raise HTTPException(
                status_code=401,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except JWTError as e:
        logger.error(f"JWT decode failed: {e}")
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    db = get_db()
    internal_user = get_or_create_internal_user(db, username)

    return CurrentUser(
        id=internal_user,
        username=username,
        role=role,
    )


@app.api_route("/api/files/raw/{catalog_id}", methods=["GET", "HEAD"])
async def get_file_raw(
    catalog_id: int,
    request: Request,
    current_user: Annotated[User, Depends(get_current_user_optional)]
):
    """Get the raw file bytes from MinIO for embedding in viewers (PDF, DOCX, etc.)"""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Check if user has access to this file
        if current_user.role == 'admin':
            cursor.execute("SELECT * FROM minio_data_catalog WHERE catalog_id = %s", (catalog_id,))
        else:
            cursor.execute(
                "SELECT * FROM minio_data_catalog WHERE catalog_id = %s AND uploaded_by = %s",
                (catalog_id, current_user.id)
            )

        file_info = cursor.fetchone()

        if not file_info:
            raise HTTPException(status_code=404, detail="File not found or access denied")

        bucket_name = file_info['bucket_name']
        object_name = file_info['object_name']
        file_format = (file_info['file_format'] or '').lower()

        # Get file from MinIO
        try:
            response = minio_client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
        except Exception as e:
            logger.error(f"MinIO read failed: {e}")
            raise HTTPException(status_code=404, detail="File not found in MinIO")

        # Determine content type
        content_type_map = {
            'pdf': 'application/pdf',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'doc': 'application/msword',
            'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'ppt': 'application/vnd.ms-powerpoint',
            'txt': 'text/plain',
            'csv': 'text/csv',
            'json': 'application/json',
        }
        content_type = content_type_map.get(file_format, 'application/octet-stream')

        # HEAD request — return headers only, no body (used by PDF viewer to check size)
        if request.method == "HEAD":
            return Response(
                content=b"",
                media_type=content_type,
                headers={"Content-Length": str(len(data))},
            )

        return Response(content=data, media_type=content_type)

    finally:
        cursor.close()
        conn.close()


# ==================== FILE UPLOAD ====================
@app.get("/api/admin/audit-logs")
def get_audit_logs(current_user: User = Depends(get_current_user)):
    """
    Return real audit log entries from the audit_logs table.
    Covers: LOGIN, UPLOAD, DELETE_FILE, DELETE_USER, TRIGGER_JOB, and any
    other action recorded by log_audit() throughout the application.
    """
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Admin privileges required")

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT
                al.id,
                al.action,
                al.details,
                al.ip_address,
                al.created_at,
                al.user_id,
                u.username
            FROM audit_logs al
            LEFT JOIN users u ON u.id = al.user_id
            ORDER BY al.created_at DESC
        """)
        logs = cursor.fetchall()
        return [dict(row) for row in logs]
    finally:
        cursor.close()
        conn.close()

# ==================== USER MANAGEMENT ====================

@app.get("/api/admin/users", tags=["Admin"])
def get_users(current_user: User = Depends(get_current_user)):
    """
    List all users who have logged in via LDAP.
    Role is managed by LDAP (gidNumber), not stored in this table —
    so we return what we have: id, username, created_at.
    """
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Admin privileges required")

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT id, username, created_at
            FROM users
            ORDER BY created_at DESC
        """)
        users = [dict(r) for r in cursor.fetchall()]
        return {"users": users, "total": len(users)}
    finally:
        cursor.close()
        conn.close()


@app.delete("/api/admin/users/{user_id}", tags=["Admin"])
def delete_user(user_id: int, current_user: User = Depends(get_current_user)):
    """Remove a user record from the internal DB (does not affect LDAP)."""
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail="Admin privileges required")
    if current_user.id == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")

    conn = get_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT username FROM users WHERE id = %s", (user_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        log_audit(current_user.id, "DELETE_USER", f"Removed user: {row['username']}")
        return {"status": "deleted", "user_id": user_id}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
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

    log_audit(
        current_user.id,
        "UPLOAD",
        f"Uploaded {len(uploaded_files)} file(s): {', '.join(uploaded_files)}"
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