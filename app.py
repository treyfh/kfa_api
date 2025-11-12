# app.py — KFA API (projects, clients, file storage with optional Google Drive)
# Version 1.0.9
# - Adds /debug/version to verify the deployed file + version + mtime
# - Logs version at startup
# - Keeps: Drive/local auto-switch, uploads, import-from-url, list, download, delete
# - Project: search, by-id, by-number(ILIKE), upsert, delete
# - Debug routes left open for setup convenience

import os
import io
import uuid
import logging
import mimetypes
import pathlib
import time
from typing import Optional, Tuple

import requests
import psycopg2
from psycopg2.pool import SimpleConnectionPool

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
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY", "dev")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Google Drive config
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  # ID only
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # e.g. /etc/secrets/deep_files.json
GDRIVE_PUBLIC = os.getenv("GDRIVE_PUBLIC", "0").lower() in {"1", "true", "yes"}

# CORS
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")
if not ALLOW_ORIGINS or ALLOW_ORIGINS.strip() == "":
    ALLOW_ORIGINS = "*"
if ALLOW_ORIGINS != "*":
    ALLOW_ORIGINS = [o.strip() for o in ALLOW_ORIGINS.split(",") if o.strip()]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kfa_api")

def _mask(s: Optional[str]) -> str:
    return (s[:6] + "..." + s[-6:]) if s else "None"

# ---------------- FastAPI ----------------
APP_VERSION = "1.0.9"
app = FastAPI(title="KFA API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS if isinstance(ALLOW_ORIGINS, list) else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve local files for previews
os.makedirs("uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

# ---------------- DB Pool ----------------
_POOL: Optional[SimpleConnectionPool] = None

def _ensure_database_url():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

@app.on_event("startup")
def _startup():
    global _POOL
    logger.info(
        "Starting KFA API v%s | API_KEY=%s | DB_URL=%s",
        APP_VERSION, _mask(API_KEY), _mask(DATABASE_URL)
    )
    try:
        _ensure_database_url()
        _POOL = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
        logger.info("DB connection pool initialized")
    except Exception as e:
        _POOL = None
        logger.error("DB pool init failed: %s", e)

@app.on_event("shutdown")
def _shutdown():
    global _POOL
    if _POOL:
        _POOL.closeall()
        logger.info("DB connection pool closed")

def db():
    global _POOL
    if _POOL is None:
        _ensure_database_url()
        return psycopg2.connect(DATABASE_URL)
    return _POOL.getconn()

def close_conn(conn):
    global _POOL
    if conn is None:
        return
    if _POOL:
        try:
            _POOL.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    else:
        try:
            conn.close()
        except Exception:
            pass

def rows_to_dicts(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ---------------- Storage Mode & Drive Helpers ----------------
_drive_svc = None
_last_drive_probe: Tuple[bool, str] = (False, "not_checked")

def _drive_env_ready() -> Tuple[bool, str]:
    if not _GOOGLE_AVAILABLE:
        return False, "google_libs_unavailable"
    if not (GDRIVE_FOLDER_ID and GOOGLE_APPLICATION_CREDENTIALS):
        return False, "missing_env"
    if not pathlib.Path(GOOGLE_APPLICATION_CREDENTIALS).exists():
        return False, "creds_file_missing"
    return True, "env_ok"

def _drive():
    global _drive_svc
    if _drive_svc:
        return _drive_svc
    ok, _ = _drive_env_ready()
    if not ok:
        raise RuntimeError("Google Drive not configured properly.")
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
        ],
    )
    _drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_svc

def _probe_drive() -> Tuple[bool, str]:
    """Try to list once inside the configured folder to confirm access."""
    global _last_drive_probe
    ok_env, reason = _drive_env_ready()
    if not ok_env:
        _last_drive_probe = (False, reason)
        return _last_drive_probe
    try:
        svc = _drive()
        q = f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        svc.files().list(q=q, pageSize=1, fields="files(id)").execute()
        _last_drive_probe = (True, "ok")
    except Exception as e:
        _last_drive_probe = (False, f"drive_list_failed: {e}")
    return _last_drive_probe

def storage_mode() -> str:
    ok, _ = _probe_drive()
    return "drive" if ok else "local"

def _infer_mime(filename: str, fallback: str = "application/octet-stream") -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or fallback

def _drive_upload(name: str, content: bytes, mime: Optional[str]):
    svc = _drive()
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime or "application/octet-stream", resumable=False)
    meta = {"name": name, "parents": [GDRIVE_FOLDER_ID]}
    f = svc.files().create(body=meta, media_body=media, fields="id, webViewLink, webContentLink").execute()
    if GDRIVE_PUBLIC:
        try:
            svc.permissions().create(fileId=f["id"], body={"role": "reader", "type": "anyone"}, fields="id").execute()
        except Exception as e:
            logger.warning("Drive permission set failed: %s", e)
    return f

def _drive_file_links(file_id: str):
    svc = _drive()
    f = svc.files().get(fileId=file_id, fields="id, webViewLink, webContentLink, mimeType, name").execute()
    return {
        "view": f.get("webViewLink"),
        "download": f.get("webContentLink"),
        "name": f.get("name"),
        "mime": f.get("mimeType"),
    }

# ---------------- Open routes ----------------
@app.get("/")
def root():
    return {"service": "KFA API", "version": APP_VERSION, "storage": storage_mode()}

@app.get("/health")
def health():
    return {"ok": True, "storage": storage_mode()}

@app.get("/debug/version")
def debug_version():
    try:
        fn = __file__
    except Exception:
        fn = "unknown"
    try:
        mtime = os.path.getmtime(__file__)
        mtime_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime))
    except Exception:
        mtime_iso = None
    return {"version": APP_VERSION, "file": fn, "file_mtime_utc": mtime_iso}

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

@app.get("/debug/storage")
def debug_storage():
    p = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    ok, reason = _probe_drive()
    return {
        "GOOGLE_AVAILABLE": _GOOGLE_AVAILABLE,
        "GDRIVE_FOLDER_ID_present": bool(GDRIVE_FOLDER_ID),
        "GOOGLE_APPLICATION_CREDENTIALS": p,
        "creds_exists": os.path.exists(p) if p else False,
        "mode_now": "drive" if ok else "local",
        "probe_reason": reason,
    }

@app.get("/debug/drive-ping")
def debug_drive_ping():
    try:
        if storage_mode() != "drive":
            return {"ok": False, "reason": "not_in_drive_mode"}
        svc = _drive()
        q = f"'{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        r = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        return {"ok": True, "sample": r.get("files", [])}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------------- Auth middleware ----------------
def _extract_api_key(request: Request):
    hdr = request.headers.get("x-api-key") or request.headers.get("X-API-Key")
    if hdr:
        return hdr
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    qp = request.query_params.get("key")
    return qp

OPEN_PATH_PREFIXES = (
    "/uploads",
    "/docs",
    "/openapi.json",
    "/redoc",
)
OPEN_PATHS_EXACT = {"/", "/health", "/debug/version", "/debug/auth", "/debug/db", "/debug/storage", "/debug/drive-ping"}

@app.middleware("http")
async def require_key(request: Request, call_next):
    path = request.url.path
    if (path in OPEN_PATHS_EXACT) or any(path.startswith(p) for p in OPEN_PATH_PREFIXES):
        return await call_next(request)
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)

# ---------------- Data models ----------------
class ProjectUpsert(BaseModel):
    number: str
    name: Optional[str] = None
    status: Optional[str] = None
    client_id: Optional[int] = None
    start_year: Optional[int] = None
    completion_year: Optional[int] = None
    address: Optional[str] = None

class ClientUpsert(BaseModel):
    name: str

class FileImportBody(BaseModel):
    url: str
    filename: Optional[str] = None
    content_type: Optional[str] = None

# ---------------- Helpers ----------------
def _get_project_id(cur, number: str) -> Optional[int]:
    cur.execute("select id from projects where number=%s", (number,))
    row = cur.fetchone()
    return row[0] if row else None

# ---------------- Project & Client Routes ----------------
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

@app.get("/projects/by-id/{pid}")
def get_project_by_id(pid: int):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            select id, number, name, status, start_year, completion_year, address
            from projects where id=%s
        """, (pid,))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "not_found"}, status_code=404)
        keys = ["id","number","name","status","start_year","completion_year","address"]
        return dict(zip(keys, row))
    finally:
        cur.close(); close_conn(conn)

@app.get("/projects/by-number/{number}")
def get_project_by_number(number: str):
    """
    Forgiving: behaves like search on 'number' and returns the newest match.
    """
    conn = db(); cur = conn.cursor()
    try:
        q = f"%{number.strip()}%"
        cur.execute("""
            select id, number, name, status, start_year, completion_year, address
            from projects
            where number ilike %s
            order by id desc
            limit 1
        """, (q,))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "not_found"}, status_code=404)
        keys = ["id","number","name","status","start_year","completion_year","address"]
        return dict(zip(keys, row))
    finally:
        cur.close(); close_conn(conn)

@app.post("/projects/upsert-by-number")
def upsert_project_by_number(data: ProjectUpsert, request: Request):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select id from projects where number=%s", (data.number,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            sets, params = [], []
            for field in ["name","status","client_id","start_year","completion_year","address"]:
                val = getattr(data, field)
                if val is not None:
                    sets.append(f"{field}=%s"); params.append(val)
            if sets:
                params.append(pid)
                cur.execute(f"update projects set {', '.join(sets)} where id=%s", tuple(params))
                conn.commit()
            return {"ok": True, "id": pid, "number": data.number}
        else:
            cur.execute("""
                insert into projects (number, name, status, client_id, start_year, completion_year, address)
                values (%s,%s,%s,%s,%s,%s,%s) returning id
            """, (data.number, data.name, data.status, data.client_id, data.start_year, data.completion_year, data.address))
            pid = cur.fetchone()[0]
            conn.commit()
            return {"ok": True, "id": pid, "number": data.number}
    except Exception as e:
        conn.rollback()
        return JSONResponse({"error": "upsert_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

@app.delete("/projects/delete-by-number/{number}")
def delete_project_by_number(number: str, request: Request):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("delete from projects where number=%s returning id", (number,))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return {"ok": True, "deleted_project_number": number}
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

@app.post("/clients/upsert-by-name")
def upsert_client_by_name(data: ClientUpsert, request: Request):
    if _extract_api_key(request) != API_KEY:
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
    except Exception as e:
        conn.rollback()
        return JSONResponse({"error": "client_upsert_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

# ---------------- File upload ----------------
@app.post("/projects/{number}/files")
async def upload_project_file(number: str, file: UploadFile = File(...), request: Request = None):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)

        data = await file.read()
        fname = file.filename or f"upload-{uuid.uuid4().hex}"
        mime = (file.content_type or _infer_mime(fname)).split(";")[0].strip()

        if storage_mode() == "drive":
            meta = _drive_upload(fname, data, mime)
            drive_id = meta["id"]
            cur.execute("""
                insert into project_files (project_id, filename, content_type, drive_id, local_path)
                values (%s, %s, %s, %s, %s) returning id
            """, (pid, fname, mime, drive_id, None))
            fid = cur.fetchone()[0]
            conn.commit()
            return {
                "ok": True,
                "file_id": fid,
                "filename": fname,
                "storage": "drive",
                "fileId": drive_id,
                "download": meta.get("webContentLink"),
                "preview": meta.get("webViewLink"),
            }

        # local fallback
        ext = os.path.splitext(fname)[1] or ".bin"
        safe_name = f"{number}_{uuid.uuid4().hex}{ext}"
        local_path = os.path.join("uploads", os.path.basename(safe_name))
        with open(local_path, "wb") as f:
            f.write(data)
        cur.execute("""
            insert into project_files (project_id, filename, content_type, drive_id, local_path)
            values (%s, %s, %s, %s, %s) returning id
        """, (pid, fname, mime, None, local_path))
        fid = cur.fetchone()[0]
        conn.commit()
        preview = f"/uploads/{os.path.basename(local_path)}" if (mime or "").startswith("image/") else None
        return {
            "ok": True,
            "file_id": fid,
            "filename": fname,
            "storage": "local",
            "download": f"/projects/{number}/files/local/{fid}",
            "preview": preview,
        }
    except Exception as e:
        conn.rollback()
        return JSONResponse({"error": "upload_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

# ---------------- File import (from URL) ----------------
@app.post("/projects/{number}/files/import-from-url")
def import_file_from_url(number: str, data: FileImportBody, request: Request):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)

        resp = requests.get(data.url, stream=True, timeout=30)
        resp.raise_for_status()

        fname = data.filename or os.path.basename(data.url.split("?")[0]) or f"import-{uuid.uuid4().hex}"
        mime = (data.content_type or resp.headers.get("Content-Type") or _infer_mime(fname)).split(";")[0].strip()
        content = resp.content

        if storage_mode() == "drive":
            meta = _drive_upload(fname, content, mime)
            drive_id = meta["id"]
            cur.execute("""
                insert into project_files (project_id, filename, content_type, drive_id, local_path)
                values (%s, %s, %s, %s, %s) returning id
            """, (pid, fname, mime, drive_id, None))
            fid = cur.fetchone()[0]
            conn.commit()
            return {
                "ok": True,
                "file_id": fid,
                "fileId": drive_id,
                "storage": "drive",
                "view": meta.get("webViewLink"),
                "preview": meta.get("webViewLink"),
            }

        # local save
        ext = os.path.splitext(fname)[1] or ".bin"
        safe_name = f"{number}_{uuid.uuid4().hex}{ext}"
        local_path = os.path.join("uploads", safe_name)
        with open(local_path, "wb") as f:
            f.write(content)
        cur.execute("""
            insert into project_files (project_id, filename, content_type, drive_id, local_path)
            values (%s, %s, %s, %s, %s) returning id
        """, (pid, fname, mime, None, local_path))
        fid = cur.fetchone()[0]
        conn.commit()
        preview = f"/uploads/{os.path.basename(local_path)}" if mime.startswith("image/") else None
        return {
            "ok": True,
            "file_id": fid,
            "fileId": str(fid),
            "storage": "local",
            "view": None,
            "preview": preview,
        }
    except Exception as e:
        conn.rollback()
        return JSONResponse({"error": "import_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

# ---------------- List files ----------------
@app.get("/projects/{number}/files")
def list_project_files(number: str, request: Request):
    if _extract_api_key(request) != API_KEY:
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
            order by created_at desc nulls last, id desc
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

# ---------------- Local download route ----------------
@app.get("/projects/{number}/files/local/{fid}")
def download_local_file(number: str, fid: int, request: Request):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select id from projects where number=%s", (number,))
        proj = cur.fetchone()
        if not proj:
            return JSONResponse({"error": "project_not_found"}, status_code=404)
        pid = proj[0]
        cur.execute("""
            select filename, content_type, local_path
            from project_files
            where id=%s and project_id=%s and drive_id is null
        """, (fid, pid))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "file_not_found"}, status_code=404)
        fname, ctype, path = row
        if not path or not os.path.exists(path):
            return JSONResponse({"error": "file_missing_on_disk"}, status_code=410)
        headers = {"Content-Disposition": f'attachment; filename="{fname}"'}
        return FileResponse(path, media_type=ctype or _infer_mime(fname), headers=headers)
    except Exception as e:
        return JSONResponse({"error": "download_failed", "detail": str(e)}, status_code=500)
    finally:
        cur.close(); close_conn(conn)

# ---------------- Delete file ----------------
@app.delete("/projects/{number}/files/{file_id}")
def delete_project_file(number: str, file_id: int, request: Request):
    if _extract_api_key(request) != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    conn = db(); cur = conn.cursor()
    try:
        pid = _get_project_id(cur, number)
        if not pid:
            return JSONResponse({"error": "project_not_found"}, status_code=404)

        cur.execute("select drive_id, local_path from project_files where id=%s and project_id=%s", (file_id, pid))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "file_not_found"}, status_code=404)
        drive_id, local_path = row

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

        cur.execute("delete from project_files where id=%s and project_id=%s", (file_id, pid))
        conn.commit()
        return {"ok": True, "deleted_file_id": file_id}
    finally:
        cur.close(); close_conn(conn)
