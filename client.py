import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")  # same as in your .env for the FastAPI server
BASE = "http://127.0.0.1:8000"

def search_projects(text: str):
    r = requests.get(f"{BASE}/projects/search", params={"text": text}, headers={"x-api-key": API_KEY}, timeout=30)
    r.raise_for_status()
    return r.json()

def get_project_by_number(number: str):
    r = requests.get(f"{BASE}/projects/by-number/{number}", headers={"x-api-key": API_KEY}, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "search":
        text = " ".join(sys.argv[2:])
        data = search_projects(text)
        if not data:
            print("No matches.")
        else:
            for row in data:
                print(f"- {row['number']}: {row['name']} (status: {row.get('status') or 'n/a'})")
        return
    if len(sys.argv) == 3 and sys.argv[1] == "by-number":
        num = sys.argv[2]
        row = get_project_by_number(num)
        if not row:
            print("Not found.")
        else:
            print(row)
        return
    # interactive prompt
    print("KFA Project CLI")
    print("Commands:")
    print("  search <text>")
    print("  by-number <project_number>")
    while True:
        try:
            cmd = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            return
        if not cmd:
            continue
        parts = cmd.split()
        if parts[0] == "search" and len(parts) >= 2:
            text = " ".join(parts[1:])
            data = search_projects(text)
            if not data:
                print("No matches.")
            else:
                for row in data:
                    print(f"- {row['number']}: {row['name']} (status: {row.get('status') or 'n/a'})")
        elif parts[0] == "by-number" and len(parts) == 2:
            row = get_project_by_number(parts[1])
            if not row:
                print("Not found.")
            else:
                print(row)
        elif parts[0] in {"quit","exit"}:
            print("bye")
            return
        else:
            print("Usage: search <text> | by-number <project_number> | exit")

if __name__ == "__main__":
    main()
