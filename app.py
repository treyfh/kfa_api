import os
import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY", "dev")
print(f"[DEBUG] Loaded API_KEY: {API_KEY[:6]}...{API_KEY[-6:]} (len={len(API_KEY) if API_KEY else 0})")

app = FastAPI(title="KFA API")

def db():
    return psycopg2.connect(DATABASE_URL)

from fastapi.responses import JSONResponse

# --- Masking helper for safe logs ---
def _mask(s):
    return (s[:6] + "..." + s[-6:]) if s else "None"

# --- Debug endpoint to inspect API key on Render ---
@app.get("/debug/auth")
def debug_auth():
    return {
        "expected_api_key_mask": _mask(API_KEY),
        "expected_api_key_len": len(API_KEY) if API_KEY else 0,
    }

# --- Authentication middleware ---

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
        rows = cur.fetchall()
        keys = ["id","number","name","status"]
        return [dict(zip(keys, r)) for r in rows]
    finally:
        cur.close(); conn.close()

@app.get("/projects")
def list_projects(text: str | None = None, status: str | None = None, limit: int = 50, offset: int = 0):
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
        rows = cur.fetchall()
        keys = ["id","number","name","status"]
        return [dict(zip(keys, r)) for r in rows]
    finally:
        cur.close(); conn.close()

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
        rows = cur.fetchall()
        return [{"id": r[0], "name": r[1]} for r in rows]
    finally:
        cur.close(); conn.close()
@app.get("/projects/filter")
def filter_projects(client: str | None = None, year_from: int | None = None, year_to: int | None = None, limit: int = 100):
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
        rows = cur.fetchall()
        keys = ["id","number","name","status","start_year","completion_year","client"]
        return [dict(zip(keys, r)) for r in rows]
    finally:
        cur.close(); conn.close()
from fastapi.responses import StreamingResponse
import io, csv

@app.get("/projects/export.csv")
def export_projects_csv(text: str | None = None, status: str | None = None):
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
        rows = cur.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["number","name","status","start_year","completion_year"])
        writer.writerows(rows)
        output.seek(0)
        return StreamingResponse(iter([output.getvalue()]),
                                 media_type="text/csv",
                                 headers={"Content-Disposition": "attachment; filename=projects.csv"})
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
        rows = cur.fetchall()
        keys = ["id","number","name","status","address"]
        return [dict(zip(keys, r)) for r in rows]
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
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
from fastapi.responses import JSONResponse

# --- Masking helper for safe logs ---
def _mask(s: str | None):
    return (s[:6] + "..." + s[-6:]) if s else "None"

# --- Debug endpoint to inspect API key on Render (SAFE: masked) ---
@app.get("/debug/auth")
def debug_auth():
    return {
        "expected_api_key_mask": _mask(API_KEY),
        "expected_api_key_len": len(API_KEY) if API_KEY else 0,
    }

# --- Authentication middleware (single source of truth) ---
@app.middleware("http")
async def require_key(request: Request, call_next):
    # Allow these paths without API key
    open_paths = {"/health", "/docs", "/openapi.json", "/debug/auth"}
    if request.url.path in open_paths:
        return await call_next(request)

    # Accept header OR query param for convenience
    received = request.headers.get("x-api-key") or request.query_params.get("key")

    if received != API_KEY:
        print(
            f"[AUTH] Unauthorized | got={_mask(received)} (len={len(received) if received else 0}) "
            f"| expected={_mask(API_KEY)} (len={len(API_KEY) if API_KEY else 0})"
        )
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    return await call_next(request)

# --- Health (no key) ---
@app.get("/health")
def health():
    return {"ok": True}

# --- Optional masked log on startup ---
print(
    f"[DEBUG] Loaded API_KEY: { _mask(API_KEY) } "
    f"(len={len(API_KEY) if API_KEY else 0})"
)

