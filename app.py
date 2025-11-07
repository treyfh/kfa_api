# app.py — KFA API (projects, clients, auto-switch local/Google Drive file storage)
# Production-ready tweaks:
# - Env-driven config (API_KEY, DATABASE_URL, CORS)
# - Accepts X-API-Key header OR Authorization: Bearer
# - Connection pooling (psycopg2 SimpleConnectionPool) with graceful startup/shutdown
# - Robust health checks
# - Safer logging (masked secrets), structured-ish logs
# - Minor hardening & helpers; preserves your existing routes/behavior

import os
import io
import csv
import uuid
import logging
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2.pool import SimpleConnectionPool

import requests  # (kept because you had it; remove if unused)

from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

# --- Optional Google Drive imports (guarded) ---
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
    _GOOGLE_AVAILABLE = True
except Exception:
    _GOOGLE_AVAILABLE = False

# ---------------- Env & Logging ----------------
load_dotenv()  # harmless on Render; useful locally

DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY", "dev")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Google Drive (optional)
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# CORS (allow "*" by default, or comma-separated list via ALLOW_ORIGINS)
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")
if ALLOW_ORIGINS.strip() == "":
    ALLOW_ORIGINS = "*"
if ALLOW_ORIGINS != "*":
    ALLOW_ORIGINS = [o.strip() for o in ALLOW_ORIGINS.split(",") if o.strip()]

# Logging config
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kfa_api")

def _mask(s: Optional[str]) -> str:
    return (s[:6] + "..." + s[-6:]) if s else "None"

logger.info("Starting KFA API | API_KEY=%s | DB_URL=%s",
            _mask(API_KEY), _mask(DATABASE_URL))

# ---------------- FastAPI app ----------------
app = FastAPI(title="KFA API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS if isinstance(ALLOW_ORIGINS, list) else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve local uploads for inline previews
os.makedirs("uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

# ---------------- DB Pool Helpers ----------------
_POOL: Optional[SimpleConnectionPool] = None

def _ensure_database_url():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

@app.on_event("startup")
def _startup():
    global _POOL
    _ensure_database_url()
    # minconn=1, maxconn=5 is fine for a small Render plan; adjust as needed
    _POOL = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
    logger.info("DB connection pool initialized")

@app.on_event("shutdown")
def _shutdown():
    global _POOL
    try:
        if _POOL:
            _POOL.closeall()
            logger.info("DB connection pool closed")
    finally:
        _POOL = None

def db():
    """Get a pooled DB connection."""
    global _POOL
    if _POOL is None:
        # fallback (shouldn't happen under normal startup)
        _ensure_database_url()
        return psycopg2.connect(DATABASE_URL)
    return _POOL.getconn()

def close_conn(conn):
    """Return or close a DB connection depending on pool state."""
    global _POOL
    if conn is None:
        return
    if _POOL is None:
        try:
            conn.close()
        except Exception:
            pass
    else:
        try:
            _POOL.putconn(conn)
        except Exception:
            # if pool rejects, close hard
            try:
                conn.close()
            except Exception:
                pass

def rows_to_dicts(cur) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ---------------- Storage Mode Helpers ----------------
_drive_svc = None

def storage_mode() -> str:
    """
    Returns 'drive' if Google Drive is properly configured, else 'local'.
    """
    if not _GOOGLE_AVAILABLE:
        return "local"
    if not (GDRIVE_FOLDER_ID and GOOGLE_APPLICATION_CREDENTIALS):
        return "local"
    if not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
        return "local"
    return "drive"

def _drive():
    global _drive_svc
    if _drive_svc:
        return _drive_svc
    if storage_mode() != "drive":
        raise RuntimeError("Google Drive not configured properly.")
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    _drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_svc

def _drive_upload(name: str, content: bytes, mime: Optional[str]):
    svc = _drive()
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime or "application/octet-stream", resumable=False)
    file_meta = {"name": name, "parents": [GDRIVE_FOLDER_ID]}
    f = svc.files().create(body=file_meta, media_body=media, fields="id, webViewLink, webContentLink").execute()
    return f  # dict with id, webViewLink, webContentLink

def _drive_file_links(file_id: str):
    svc = _drive()
    f = svc.files().get(fileId=file_id, fields="id, webViewLink, webContentLink, mimeType, name").execute()
    return {
        "view": f.get("webViewLink"),
        "download": f.get("webContentLink"),
        "name": f.get("name"),
        "mime": f.get("mimeType")
    }

# ---------------- Open (no key) endpoints ----------------
@app.get("/")
def root():
    return {
        "service": "KFA API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "storage": storage_mode()
    }

@app.get("/health")
def health():
    # lightweight health; DB probe is optional here (keep /debug/db for deeper test)
    return {"ok": True, "storage": storage_mode()}

@app.get("/debug/auth")
def debug_auth():
    return {"expected_api_key_mask": _mask(API_KEY)}

@app.get("/debug/db")
def debug_db():
    conn = None
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("select version()")
        v = cur.fetchone()[0]
        cur.close(); close_conn(conn); conn = None
        return {"db": "ok", "version": v}
    except Exception as e:
        if conn:
            close_conn(conn)
        return JSONResponse({"db": "error", "detail": str(e)}, status_code=500)

# ---------------- Authentication middleware ----------------
def _extract_api_key(request: Request) -> Optional[str]:
    # Accept x-api-key, X-API-Key, or Authorization: Bearer <token>, or ?key=...
    hdr = request.headers.get("x-api-key") or request.headers.get("X-API-Key")
    if hdr:
        return hdr
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    # optional query param fallback
    qp = request.query_params.get("key")
    if qp:
        return qp
    return None

@app.middleware("http")
async def require_key(request: Request, call_next):
    open_paths = {
        "/", "/health", "/docs", "/openapi.json", "/debug/auth", "/debug/db"
    }
    if request.url.path in open_paths or request.url.path.startswith("/uploads"):
        return await call_next(request)

    received = _extract_api_key(request)
    if received != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)

# ---------------- READ Endpoints ----------------
@app.get("/projects/by-number/{number}")
def get_project_by_number(number: str):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(
            "select id, number, name, status, start_year, completion_year "
            "from projects where number = %s",
            (number,)
        )
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        keys = ["id","number","name","status","start_year","completion_year"]
        return dict(zip(keys, row))
    finally:
        cur.close(); close_conn(conn)

@app.get("/projects/search")
def search_projects(text: str = ""):
    conn = db(); cur = conn.cursor()
    try:
        q = f"%{text}%"
        cur.execute("""
            select id, number, name, status
            from projects
            where number ilike %s or name ilike %s
            order by number asc limit 50
        """, (q, q))
        return rows_to_dicts(cur)
    finally:
        cur.close(); close_conn(conn)

@app.get("/clients")
def list_clients(limit: int = 200):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select id, name from clients order by name asc limit %s", (limit,))
        return rows_to_dicts(cur)
    finally:
        cur.close(); close_conn(conn)

# ---------------- WRITE: Projects & Clients ----------------
class ProjectUpsert(BaseModel):
    number: str
    name: Optional[str] = None
    status: Optional[str] = None
    client_id: Optional[int] = None
    start_year: Optional[int] = None
    completion_year: Optional[int] = None
    address: Optional[str] = None

@app.post("/projects/upsert-by-number")
def upsert_project_by_number(data: ProjectUpsert, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select id from projects where number = %s", (data.number,))
        row = cur.fetchone()
        if row:
            project_id = row[0]
            sets, params = [], []
            if data.name is not None:
                sets.append("name=%s"); params.append(data.name)
            if data.status is not None:
                sets.append("status=%s"); params.append(data.status)
            if data.client_id is not None:
                sets.append("client_id=%s"); params.append(data.client_id)
            if data.start_year is not None:
                sets.append("start_year=%s"); params.append(data.start_year)
            if data.completion_year is not None:
                sets.append("completion_year=%s"); params.append(data.completion_year)
            if data.address is not None:
                sets.append("address=%s"); params.append(data.address)
            if sets:
                sql = f"update projects set {', '.join(sets)} where id=%s"
                params.append(project_id)
                cur.execute(sql, tuple(params))
                conn.commit()
            return {"ok": True, "id": project_id, "number": data.number}
        else:
            if not data.name:
                return JSONResponse({"error": "missing_name"}, status_code=400)
            cur.execute("""
                insert into projects (number, name, status, client_id, start_year, completion_year, address)
                values (%s,%s,%s,%s,%s,%s,%s)
                returning id
            """, (data.number, data.name, data.status, data.client_id, data.start_year, data.completion_year, data.address))
            project_id = cur.fetchone()[0]
            conn.commit()
            return {"ok": True, "id": project_id, "number": data.number}
    except Exception as e:
        conn.rollback()
        logger.exception("Project upsert failed")
        return JSONResponse({"error": "upsert_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

@app.delete("/projects/delete-by-number/{number}")
def delete_project_by_number(number: str, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        cur.execute("delete from projects where number = %s returning id;", (number,))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        return {"ok": True, "deleted_project_number": number}
    finally:
        cur.close(); close_conn(conn)

class ClientUpsert(BaseModel):
    name: str

@app.post("/clients/upsert-by-name")
def upsert_client_by_name(data: ClientUpsert, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            insert into clients (name)
            values (%s)
            on conflict (name) do update set name=excluded.name
            returning id
        """, (data.name,))
        cid = cur.fetchone()[0]
        conn.commit()
        return {"ok": True, "id": cid, "name": data.name}
    finally:
        cur.close(); close_conn(conn)

# ---------------- FILE STORAGE (Auto-switch: Drive or Local) ----------------
def _get_project_id(cur, number):
    cur.execute("select id from projects where number=%s", (number,))
    row = cur.fetchone()
    return row[0] if row else None

@app.post("/projects/{number}/files")
async def upload_project_file(number: str, file: UploadFile = File(...), request: Request = None):
    """
    Upload a file (including images) for a project.
    Uses Google Drive if configured; otherwise stores locally in /uploads.
    """
    key = _extract_api_key(request) if request else None
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)

        data = await file.read()
        fname = file.filename or f"upload-{uuid.uuid4().hex}"
        mime = file.content_type or "application/octet-stream"

        if storage_mode() == "drive":
            meta = _drive_upload(fname, data, mime)
            drive_id = meta["id"]
            cur.execute("""
                insert into project_files (project_id, filename, content_type, drive_id, local_path)
                values (%s, %s, %s, %s, %s)
                returning id
            """, (pid, fname, mime, drive_id, None))
            fid = cur.fetchone()[0]
            conn.commit()

            return {
                "ok": True,
                "file_id": fid,
                "filename": fname,
                "storage": "drive",
                "download": meta.get("webContentLink"),
                "preview": meta.get("webViewLink")
            }

        # LOCAL fallback
        ext = os.path.splitext(fname)[1] or ".bin"
        safe_name = f"{number}_{uuid.uuid4().hex}{ext}"
        local_path = os.path.join("uploads", os.path.basename(safe_name))
        with open(local_path, "wb") as f:
            f.write(data)

        cur.execute("""
            insert into project_files (project_id, filename, content_type, drive_id, local_path)
            values (%s, %s, %s, %s, %s)
            returning id
        """, (pid, fname, mime, None, local_path))
        fid = cur.fetchone()[0]
        conn.commit()

        preview = None
        if mime.startswith("image/"):
            preview = f"/uploads/{os.path.basename(local_path)}"

        return {
            "ok": True,
            "file_id": fid,
            "filename": fname,
            "storage": "local",
            "download": f"/projects/{number}/files/local/{fid}",
            "preview": preview
        }
    except Exception as e:
        conn.rollback()
        logger.exception("File upload failed")
        return JSONResponse({"error": "upload_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

@app.get("/projects/{number}/files")
def list_project_files(number: str, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return []
        cur.execute("""
            select id, filename, content_type, drive_id, local_path, created_at
            from project_files
            where project_id=%s
            order by created_at desc
        """, (pid,))
        rows = rows_to_dicts(cur)

        mode = storage_mode()
        out = []
        for r in rows:
            item = dict(r)
            if mode == "drive" and r.get("drive_id"):
                try:
                    links = _drive_file_links(r["drive_id"])
                    item["download"] = links["download"]
                    if (r.get("content_type") or "").startswith("image/"):
                        item["preview"] = links["view"]
                except Exception:
                    item["download"] = None
            else:
                item["download"] = f"/projects/{number}/files/local/{r['id']}"
                if (r.get("content_type") or "").startswith("image/") and r.get("local_path"):
                    item["preview"] = f"/uploads/{os.path.basename(r['local_path'])}"
            out.append(item)

        return out
    finally:
        cur.close(); close_conn(conn)

@app.get("/projects/{number}/files/local/{file_id}")
def download_local_file(number: str, file_id: int, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)
        cur.execute(
            "select filename, local_path, content_type from project_files where id=%s and project_id=%s",
            (file_id, pid)
        )
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "file_not_found"}, status_code=404)
        filename, path, mime = row
        return FileResponse(path, media_type=mime or "application/octet-stream", filename=filename)
    finally:
        cur.close(); close_conn(conn)

@app.delete("/projects/{number}/files/{file_id}")
def delete_project_file(number: str, file_id: int, request: Request):
    key = _extract_api_key(request)
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)

        # Get storage references first
        cur.execute("select drive_id, local_path from project_files where id=%s and project_id=%s", (file_id, pid))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "file_not_found"}, status_code=404)
        drive_id, local_path = row

        # Delete remote/local
        if drive_id and storage_mode() == "drive":
            try:
                _drive().files().delete(fileId=drive_id).execute()
            except Exception:
                pass
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass

        # Remove DB row
        cur.execute("delete from project_files where id=%s and project_id=%s", (file_id, pid))
        conn.commit()
        return {"ok": True, "deleted_file_id": file_id}
    finally:
        cur.close(); close_conn(conn)
