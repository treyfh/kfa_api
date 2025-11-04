# app.py  — KFA API (clean version)
import os
import io
import csv
import psycopg2
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ---------------- Env & App ----------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY", "dev")  # default only for local; set real key in Render

app = FastAPI(title="KFA API")

# CORS (you can replace "*" with your domain once deployed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- DB Helpers ----------------
def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)

def rows_to_dicts(cur) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ---------------- Masked logging helpers ----------------
def _mask(s: Optional[str]) -> str:
    return (s[:6] + "..." + s[-6:]) if s else "None"

print(f"[DEBUG] Loaded API_KEY: {_mask(API_KEY)} (len={len(API_KEY) if API_KEY else 0})")

# ---------------- Open (no key) endpoints ----------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/debug/auth")
def debug_auth():
    return {
        "expected_api_key_mask": _mask(API_KEY),
        "expected_api_key_len": len(API_KEY) if API_KEY else 0,
    }

@app.get("/debug/db")
def debug_db():
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("select version()")
        v = cur.fetchone()[0]
        cur.close(); conn.close()
        return {"db": "ok", "version": v}
    except Exception as e:
        return JSONResponse(
            {"db": "error", "detail": str(e), "type": e.__class__.__name__},
            status_code=500
        )

# ---------------- Authentication middleware (single) ----------------
@app.middleware("http")
async def require_key(request: Request, call_next):
    # Paths that DO NOT require API key:
    open_paths = {"/health", "/docs", "/openapi.json", "/debug/auth", "/debug/db"}
    if request.url.path in open_paths:
        return await call_next(request)

    # Accept header OR ?key= query param
    received = request.headers.get("x-api-key") or request.query_params.get("key")
    if received != API_KEY:
        print(
            f"[AUTH] Unauthorized | got={_mask(received)} (len={len(received) if received else 0}) "
            f"| expected={_mask(API_KEY)} (len={len(API_KEY) if API_KEY else 0})"
        )
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    return await call_next(request)

# ---------------- Project & Client Endpoints ----------------
@app.get("/projects/by-number/{number}")
def get_project_by_number(number: str):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            select id, number, name, status, start_year, completion_year
            from projects
            where number = %s
        """, (number,))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        keys = ["id","number","name","status","start_year","completion_year"]
        return dict(zip(keys, row))
    finally:
        cur.close(); conn.close()

@app.get("/projects/search")
def search_projects(text: str):
    conn = db(); cur = conn.cursor()
    try:
        q = f"%{text}%"
        cur.execute("""
            select id, number, name, status
            from projects
            where number ilike %s or name ilike %s
            order by number asc
            limit 50
        """, (q, q))
        rows = rows_to_dicts(cur)
        return rows
    finally:
        cur.close(); conn.close()

@app.get("/projects")
def list_projects(text: Optional[str] = None,
                  status: Optional[str] = None,
                  limit: int = 50,
                  offset: int = 0):
    conn = db(); cur = conn.cursor()
    try:
        clauses, params = [], []
        if text:
            clauses.append("(number ilike %s or name ilike %s)")
            params += [f"%{text}%", f"%{text}%"]
        if status:
            clauses.append("status ilike %s")
            params.append(status)
        where = (" where " + " and ".join(clauses)) if clauses else ""
        cur.execute(f"""
            select id, number, name, status
            from projects
            {where}
            order by number asc
            limit %s offset %s
        """, (*params, limit, offset))
        return rows_to_dicts(cur)
    finally:
        cur.close(); conn.close()

# NOTE: singular to avoid route conflicts
@app.get("/project/{project_id}")
def get_project(project_id: int):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            select id, number, name, status, start_year, completion_year
            from projects where id = %s
        """, (project_id,))
        row = cur.fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        keys = ["id","number","name","status","start_year","completion_year"]
        return dict(zip(keys, row))
    finally:
        cur.close(); conn.close()

@app.get("/clients")
def list_clients(limit: int = 200):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select id, name from clients order by name asc limit %s", (limit,))
        rows = rows_to_dicts(cur)
        return rows
    finally:
        cur.close(); conn.close()

@app.get("/projects/filter")
def filter_projects(client: Optional[str] = None,
                    year_from: Optional[int] = None,
                    year_to: Optional[int] = None,
                    limit: int = 100):
    conn = db(); cur = conn.cursor()
    try:
        clauses, params = [], []
        if client:
            clauses.append("c.name ilike %s")
            params.append(client)
        if year_from is not None:
            clauses.append("(p.start_year is not null and p.start_year >= %s)")
            params.append(year_from)
        if year_to is not None:
            clauses.append("(p.completion_year is not null and p.completion_year <= %s)")
            params.append(year_to)
        where = " where " + " and ".join(clauses) if clauses else ""
        cur.execute(f"""
            select p.id, p.number, p.name, p.status, p.start_year, p.completion_year, c.name as client
            from projects p
            left join clients c on c.id = p.client_id
            {where}
            order by p.number asc
            limit %s
        """, (*params, limit))
        rows = rows_to_dicts(cur)
        return rows
    finally:
        cur.close(); conn.close()

@app.get("/projects/export.csv")
def export_projects_csv(text: Optional[str] = None, status: Optional[str] = None):
    conn = db(); cur = conn.cursor()
    try:
        clauses, params = [], []
        if text:
            clauses.append("(number ilike %s or name ilike %s)")
            params += [f"%{text}%", f"%{text}%"]
        if status:
            clauses.append("status ilike %s")
            params.append(status)
        where = " where " + " and ".join(clauses) if clauses else ""
        cur.execute(f"""
            select number, name, status, start_year, completion_year
            from projects
            {where}
            order by number asc
            limit 1000
        """, tuple(params))
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["number","name","status","start_year","completion_year"])
        writer.writerows(cur.fetchall())
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=projects.csv"}
        )
    finally:
        cur.close(); conn.close()

@app.get("/projects/address-search")
def address_search(q: str, limit: int = 50):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            select id, number, name, status, address
            from projects
            where address ilike %s
            order by number asc
            limit %s
        """, (f"%{q}%", limit))
        return rows_to_dicts(cur)
    finally:
        cur.close(); conn.close()

@app.get("/projects/stats")
def projects_stats():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("select status, count(*) from projects group by status order by 2 desc")
        by_status = [{"status": r[0], "count": r[1]} for r in cur.fetchall()]
        cur.execute("""
            select coalesce(start_year, completion_year) as yr, count(*)
            from projects
            where coalesce(start_year, completion_year) is not null
            group by 1 order by 1
        """)
        by_year = [{"year": r[0], "count": r[1]} for r in cur.fetchall()]
        return {"by_status": by_status, "by_year": by_year}
    finally:
        cur.close(); conn.close()
# ---------- WRITE ACCESS: Project Upsert by Number ----------
from pydantic import BaseModel

class ProjectUpsert(BaseModel):
    number: str
    name: str | None = None
    status: str | None = None
    client_id: int | None = None
    start_year: int | None = None
    completion_year: int | None = None
    address: str | None = None

@app.post("/projects/upsert-by-number")
def upsert_project_by_number(data: ProjectUpsert, request: Request):
    # Require API key (same protection as other endpoints)
    key = request.headers.get("x-api-key") or request.query_params.get("key")
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db(); cur = conn.cursor()
    try:
        # Ensure table has a unique index on number (safe if already exists)
        cur.execute("""
            create unique index if not exists projects_number_uidx on projects (number);
        """)
        conn.commit()

        # Upsert by project number
        cur.execute("""
            insert into projects (number, name, status, client_id, start_year, completion_year, address)
            values (%s,%s,%s,%s,%s,%s,%s)
            on conflict (number) do update set
              name = coalesce(excluded.name, projects.name),
              status = coalesce(excluded.status, projects.status),
              client_id = coalesce(excluded.client_id, projects.client_id),
              start_year = coalesce(excluded.start_year, projects.start_year),
              completion_year = coalesce(excluded.completion_year, projects.completion_year),
              address = coalesce(excluded.address, projects.address)
            returning id
        """, (data.number, data.name, data.status, data.client_id,
              data.start_year, data.completion_year, data.address))
        new_id = cur.fetchone()[0]
        conn.commit()
        return {"ok": True, "id": new_id, "number": data.number}
    finally:
        cur.close(); conn.close()
from fastapi import Request

@app.delete("/projects/delete-by-number/{number}")
def delete_project_by_number(number: str, request: Request):
    # Auth (if your middleware already enforces it, keep this anyway for clarity)
    key = request.headers.get("x-api-key") or request.query_params.get("key")
    if key != API_KEY:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute("delete from projects where number = %s returning id;", (number,))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        return {"ok": True, "deleted_project_number": number}
    finally:
        cur.close()
        conn.close()
