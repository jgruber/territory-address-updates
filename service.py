#!/usr/bin/env python3
"""
service.py  –  Territory Address Update REST API

Start with:
    uvicorn service:app --host 0.0.0.0 --port 8000

Endpoints
---------
GET  /                          Service status and file readiness
GET  /users                     List usernames
POST /users                     Create a user  { "username": "...", "password": "..." }
PUT  /users/{username}/password Change a password  { "password": "..." }
DEL  /users/{username}          Delete a user

POST /upload/shapefile          Upload the parcel shapefile ZIP
POST /upload/territories        Upload Territories.csv
POST /upload/addresses          Upload TerritoryAddresses.csv
GET  /upload/status             Show which files are present

POST /update                    Start the update job (runs in background)
GET  /update/status             Poll job status and view log

GET  /download/addresses        Download the updated TerritoryAddresses.csv
GET  /download/report           Download the latest update report CSV

DELETE /files                   Delete all uploaded files and reports

GET  /query/street?q=...        Search the shapefile for a street name
                                  e.g. ?q=Jupiter+Rd  or  ?q=Edgewood+Ln,+Allen
"""

import asyncio
import csv
import io
import json
import os
import re
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE      = os.path.dirname(os.path.abspath(__file__))
NWS_DIR   = os.path.join(BASE, "data", "NWS")
CAD_DIR   = os.path.join(BASE, "data", "CAD")
OFF_DIR   = os.path.join(BASE, "data", "OFF")

CREDENTIALS_FILE    = os.path.join(BASE, "data", "credentials.json")
CREDENTIALS_DEFAULT = os.path.join(BASE, "credentials.json")
TERRITORIES_CSV  = os.path.join(NWS_DIR, "Territories.csv")
ADDRESSES_CSV    = os.path.join(NWS_DIR, "TerritoryAddresses.csv")
PERSONS_CSV      = os.path.join(NWS_DIR, "Persons.csv")
STATUS_CSV       = os.path.join(NWS_DIR, "Status.csv")
OFF_FILE         = os.path.join(OFF_DIR, "Address.txt")
CHANGES_CSV      = os.path.join(NWS_DIR, "TerritoryAddressesChanges.csv")
UPDATE_SCRIPT    = os.path.join(BASE, "update_territory_addresses.py")


def _find_shapefile() -> Optional[str]:
    """Return path to the first .zip found in CAD_DIR, or None."""
    if not os.path.isdir(CAD_DIR):
        return None
    for name in sorted(os.listdir(CAD_DIR)):
        if name.endswith(".zip"):
            return os.path.join(CAD_DIR, name)
    return None


def _init_credentials() -> None:
    """Write default credentials to data/credentials.json on first start."""
    if not os.path.exists(CREDENTIALS_FILE):
        os.makedirs(os.path.dirname(CREDENTIALS_FILE), exist_ok=True)
        with open(CREDENTIALS_DEFAULT) as src, open(CREDENTIALS_FILE, "w") as dst:
            dst.write(src.read())


_init_credentials()


def _load_users() -> dict:
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)


def _save_users(users: dict) -> None:
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(users, f, indent=2)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
security = HTTPBasic(auto_error=False)

# Use a non-Basic scheme so the browser never shows its native auth popup.
_AUTH_HEADER = {"WWW-Authenticate": "x-basic"}


def authenticate(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> str:
    """Validate HTTP Basic credentials and return the authenticated username."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers=_AUTH_HEADER,
        )
    users = _load_users()
    if credentials.username not in users or users[credentials.username] != credentials.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers=_AUTH_HEADER,
        )
    return credentials.username


# ---------------------------------------------------------------------------
# Update job state
# ---------------------------------------------------------------------------
class _UpdateState:
    def __init__(self):
        self.status: str = "idle"        # idle | running | completed | failed
        self.log: str = ""
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self.report_file: Optional[str] = None
        self.overwrite: bool = True
        self.split: bool = False
        self.split_rows: int = 200
        self.split_files: List[str] = []


_update_state = _UpdateState()
_update_lock = threading.Lock()


_SPLIT_FILE_RE = re.compile(r"^(.+)_(\d+)\.csv$")


def _do_split(csv_path: str, max_rows: int) -> List[str]:
    """Split a CSV file into numbered chunks. Returns list of generated filenames.
    Removes any pre-existing split files for the same base name first.
    """
    p = Path(csv_path)
    stem_escaped = re.escape(p.stem)
    # Remove old split files
    for old in p.parent.glob(f"{p.stem}_*.csv"):
        if re.match(rf"^{stem_escaped}_\d+\.csv$", old.name):
            old.unlink()
    if not p.exists():
        return []
    with open(p, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    generated = []
    for idx, start in enumerate(range(0, max(len(rows), 1), max_rows), 1):
        chunk = rows[start:start + max_rows]
        out_path = p.parent / f"{p.stem}_{idx}{p.suffix}"
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk)
        generated.append(out_path.name)
    return generated


def _clear_all_split_files() -> List[str]:
    """Remove all split CSV files from NWS_DIR. Returns names of deleted files."""
    deleted = []
    if not os.path.isdir(NWS_DIR):
        return deleted
    for name in os.listdir(NWS_DIR):
        if _SPLIT_FILE_RE.match(name):
            os.remove(os.path.join(NWS_DIR, name))
            deleted.append(name)
    return deleted


def _run_update_job() -> None:
    """Run update_territory_addresses.py as a subprocess, capture output."""
    try:
        cmd = [sys.executable, UPDATE_SCRIPT]
        if not _update_state.overwrite:
            cmd.append("--no-overwrite")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=BASE,
        )
        _update_state.log = result.stdout + (result.stderr or "")
        _update_state.status = "completed" if result.returncode == 0 else "failed"

        # Run split if requested and update succeeded
        _update_state.split_files = []
        if _update_state.split and result.returncode == 0:
            split_files = []
            candidates = []
            if _update_state.overwrite:
                candidates.append(ADDRESSES_CSV)
            candidates.append(CHANGES_CSV)
            for csv_path in candidates:
                names = _do_split(csv_path, _update_state.split_rows)
                split_files.extend(names)
                _update_state.log += f"\nSplit {Path(csv_path).name} into {len(names)} file(s)."
            _update_state.split_files = split_files
    except Exception as exc:
        _update_state.status = "failed"
        _update_state.log = f"Failed to start update process: {exc}"
    finally:
        _update_state.completed_at = datetime.now().isoformat()
        # Find the latest report written by the script
        try:
            reports = sorted(
                f for f in os.listdir(NWS_DIR) if f.startswith("update_report_")
            )
            _update_state.report_file = reports[-1] if reports else None
        except OSError:
            _update_state.report_file = None
        _update_lock.release()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Territory Address Update Service",
    description="Upload shapefiles and CSV data, run the address update, download results.",
)

STATIC_DIR = os.path.join(BASE, "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# Root – serve web UI
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_ui():
    html_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>UI not found</h1>", status_code=404)
    with open(html_path) as f:
        return f.read()


# ---------------------------------------------------------------------------
# Status – service status (JSON)
# ---------------------------------------------------------------------------
@app.get("/status", summary="Service status")
def service_status(_user: str = Depends(authenticate)):
    return {
        "service": "Territory Address Update",
        "files": {
            "shapefile":       _find_shapefile() is not None,
            "territories_csv": os.path.exists(TERRITORIES_CSV),
            "addresses_csv":   os.path.exists(ADDRESSES_CSV),
        },
        "ready_to_update": all([
            _find_shapefile() is not None,
            os.path.exists(TERRITORIES_CSV),
            os.path.exists(ADDRESSES_CSV),
        ]),
        "update_status": _update_state.status,
    }


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------
class _UserCreate(BaseModel):
    username: str
    password: str


class _PasswordChange(BaseModel):
    password: str


@app.get("/users", summary="List all usernames")
def list_users(_user: str = Depends(authenticate)):
    return {"users": list(_load_users().keys())}


@app.post("/users", status_code=status.HTTP_201_CREATED, summary="Create a user")
def create_user(body: _UserCreate, _user: str = Depends(authenticate)):
    users = _load_users()
    if body.username in users:
        raise HTTPException(status_code=400, detail=f"User '{body.username}' already exists")
    users[body.username] = body.password
    _save_users(users)
    return {"message": f"User '{body.username}' created"}


@app.put("/users/{username}/password", summary="Change a user's password")
def change_password(username: str, body: _PasswordChange, _user: str = Depends(authenticate)):
    users = _load_users()
    if username not in users:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")
    users[username] = body.password
    _save_users(users)
    return {"message": f"Password updated for '{username}'"}


@app.delete("/users/{username}", summary="Delete a user")
def delete_user(username: str, current_user: str = Depends(authenticate)):
    if username == current_user:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")
    users = _load_users()
    if username not in users:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")
    del users[username]
    _save_users(users)
    return {"message": f"User '{username}' deleted"}


# ---------------------------------------------------------------------------
# File uploads
# ---------------------------------------------------------------------------
@app.post("/upload/shapefile", summary="Upload the parcel shapefile ZIP")
async def upload_shapefile(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File must be a .zip archive")
    os.makedirs(CAD_DIR, exist_ok=True)
    # Remove any existing zip before saving the new one
    existing = _find_shapefile()
    if existing:
        os.remove(existing)
    dest = os.path.join(CAD_DIR, file.filename)
    with open(dest, "wb") as f:
        f.write(await file.read())
    size = os.path.getsize(dest)
    return {"message": "Shapefile uploaded", "saved_as": file.filename, "bytes": size}


@app.post("/upload/territories", summary="Upload Territories.csv")
async def upload_territories(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    os.makedirs(NWS_DIR, exist_ok=True)
    with open(TERRITORIES_CSV, "wb") as f:
        f.write(await file.read())
    return {"message": "Territories.csv uploaded", "bytes": os.path.getsize(TERRITORIES_CSV)}


@app.post("/upload/addresses", summary="Upload TerritoryAddresses.csv")
async def upload_addresses(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    os.makedirs(NWS_DIR, exist_ok=True)
    with open(ADDRESSES_CSV, "wb") as f:
        f.write(await file.read())
    return {"message": "TerritoryAddresses.csv uploaded", "bytes": os.path.getsize(ADDRESSES_CSV)}


@app.post("/upload/persons", summary="Upload Persons.csv")
async def upload_persons(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    os.makedirs(NWS_DIR, exist_ok=True)
    with open(PERSONS_CSV, "wb") as f:
        f.write(await file.read())
    return {"message": "Persons.csv uploaded", "bytes": os.path.getsize(PERSONS_CSV)}


@app.post("/upload/status-file", summary="Upload Status.csv")
async def upload_status_file(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    os.makedirs(NWS_DIR, exist_ok=True)
    with open(STATUS_CSV, "wb") as f:
        f.write(await file.read())
    return {"message": "Status.csv uploaded", "bytes": os.path.getsize(STATUS_CSV)}


@app.post("/upload/off-file", summary="Upload Address.txt (OFF)")
async def upload_off_file(
    file: UploadFile = File(...),
    _user: str = Depends(authenticate),
):
    os.makedirs(OFF_DIR, exist_ok=True)
    with open(OFF_FILE, "wb") as f:
        f.write(await file.read())
    return {"message": "Address.txt uploaded", "bytes": os.path.getsize(OFF_FILE)}


@app.get("/upload/status", summary="Check which files are present")
def upload_status(_user: str = Depends(authenticate)):
    def _info(path: str) -> dict:
        if os.path.exists(path):
            st = os.stat(path)
            return {
                "present": True,
                "bytes": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            }
        return {"present": False}

    shapefile_path = _find_shapefile()
    return {
        "shapefile":       _info(shapefile_path) if shapefile_path else {"present": False},
        "territories_csv": _info(TERRITORIES_CSV),
        "addresses_csv":   _info(ADDRESSES_CSV),
        "persons_csv":     _info(PERSONS_CSV),
        "status_csv":      _info(STATUS_CSV),
        "off_file":        _info(OFF_FILE),
        "ready_to_update": all([
            shapefile_path is not None,
            os.path.exists(TERRITORIES_CSV),
            os.path.exists(ADDRESSES_CSV),
        ]),
    }


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------
@app.post("/update", summary="Start the territory address update job")
def trigger_update(
    overwrite: bool = True,
    split: bool = False,
    split_rows: int = 200,
    _user: str = Depends(authenticate),
):
    missing = []
    if not _find_shapefile():
        missing.append("shapefile")
    if not os.path.exists(TERRITORIES_CSV):
        missing.append("territories_csv")
    if not os.path.exists(ADDRESSES_CSV):
        missing.append("addresses_csv")
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required files: {', '.join(missing)}. Check /upload/status",
        )

    if not _update_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="An update is already running")

    if split and split_rows < 1:
        raise HTTPException(status_code=400, detail="split_rows must be a positive integer")

    _update_state.status      = "running"
    _update_state.log         = ""
    _update_state.started_at  = datetime.now().isoformat()
    _update_state.completed_at = None
    _update_state.report_file  = None
    _update_state.split_files  = []
    _update_state.overwrite    = overwrite
    _update_state.split        = split
    _update_state.split_rows   = split_rows

    threading.Thread(target=_run_update_job, daemon=True).start()

    return {"message": "Update job started", "poll": "/update/status"}


@app.get("/update/status", summary="Poll the current update job status")
def get_update_status(_user: str = Depends(authenticate)):
    return {
        "status":       _update_state.status,
        "started_at":   _update_state.started_at,
        "completed_at": _update_state.completed_at,
        "report_file":  _update_state.report_file,
        "split_files":  _update_state.split_files,
        "log":          _update_state.log,
    }


# ---------------------------------------------------------------------------
# Street query
# ---------------------------------------------------------------------------
@app.get("/query/street", summary="Search shapefile addresses by street name")
async def query_street(
    q: str = Query(..., description='Street name, e.g. "Jupiter Rd" or "Edgewood Ln, Allen"'),
    _user: str = Depends(authenticate),
):
    if not _find_shapefile():
        raise HTTPException(status_code=400, detail="Shapefile not uploaded yet. POST /upload/shapefile first.")
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' must not be empty.")

    # Import lazily so startup isn't slowed if pyshp/pyproj aren't installed yet
    from query_shape_street import search_by_street

    loop = asyncio.get_event_loop()
    try:
        results = await loop.run_in_executor(None, search_by_street, q.strip())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}")

    street_part = q.strip().partition(",")[0].strip()
    city_part   = q.strip().partition(",")[2].strip()
    return {
        "query":       q.strip(),
        "street":      street_part,
        "city_filter": city_part or None,
        "count":       len(results),
        "results":     results,
    }


# ---------------------------------------------------------------------------
# Downloads
# ---------------------------------------------------------------------------
@app.get("/download/addresses", summary="Download the updated TerritoryAddresses.csv")
def download_addresses(_user: str = Depends(authenticate)):
    if not os.path.exists(ADDRESSES_CSV):
        raise HTTPException(status_code=404, detail="TerritoryAddresses.csv not found")
    return FileResponse(
        ADDRESSES_CSV,
        media_type="text/csv",
        filename="TerritoryAddresses.csv",
    )


@app.get("/download/split/{filename}", summary="Download a split CSV file")
def download_split_file(filename: str, _user: str = Depends(authenticate)):
    if not _SPLIT_FILE_RE.match(filename) or not filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = os.path.join(NWS_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, media_type="text/csv", filename=filename)


@app.get("/download/changes", summary="Download TerritoryAddressesChanges.csv")
def download_changes(_user: str = Depends(authenticate)):
    if not os.path.exists(CHANGES_CSV):
        raise HTTPException(status_code=404, detail="TerritoryAddressesChanges.csv not found — run an update first")
    return FileResponse(
        CHANGES_CSV,
        media_type="text/csv",
        filename="TerritoryAddressesChanges.csv",
    )


@app.get("/download/report", summary="Download the latest update report CSV")
def download_report(_user: str = Depends(authenticate)):
    if not _update_state.report_file:
        raise HTTPException(status_code=404, detail="No report available — run an update first")
    path = os.path.join(NWS_DIR, _update_state.report_file)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report file no longer on disk")
    return FileResponse(path, media_type="text/csv", filename=_update_state.report_file)


# ---------------------------------------------------------------------------
# Delete all files
# ---------------------------------------------------------------------------
@app.delete("/files", summary="Delete all uploaded files and reports")
def delete_files(_user: str = Depends(authenticate)):
    if _update_state.status == "running":
        raise HTTPException(status_code=409, detail="Cannot delete files while an update is running")

    deleted = []

    shapefile_path = _find_shapefile()
    for path in filter(None, [shapefile_path, TERRITORIES_CSV, ADDRESSES_CSV, PERSONS_CSV, STATUS_CSV, OFF_FILE, CHANGES_CSV]):
        if os.path.exists(path):
            os.remove(path)
            deleted.append(os.path.basename(path))

    # Remove all report and split files
    if os.path.isdir(NWS_DIR):
        for name in os.listdir(NWS_DIR):
            if name.startswith("update_report_") or _SPLIT_FILE_RE.match(name):
                os.remove(os.path.join(NWS_DIR, name))
                deleted.append(name)

    _update_state.status      = "idle"
    _update_state.log         = ""
    _update_state.started_at  = None
    _update_state.completed_at = None
    _update_state.report_file  = None
    _update_state.split_files  = []

    return {"message": "Files deleted", "deleted": deleted}


# ---------------------------------------------------------------------------
# Delete a single file
# ---------------------------------------------------------------------------
@app.delete("/files/{file_key}", summary="Delete a single uploaded file")
def delete_file(file_key: str, _user: str = Depends(authenticate)):
    if _update_state.status == "running":
        raise HTTPException(status_code=409, detail="Cannot delete files while an update is running")

    file_map = {
        "shapefile":   _find_shapefile(),
        "territories": TERRITORIES_CSV,
        "addresses":   ADDRESSES_CSV,
        "persons":     PERSONS_CSV,
        "status":      STATUS_CSV,
        "off":         OFF_FILE,
    }

    if file_key not in file_map:
        raise HTTPException(status_code=404, detail=f"Unknown file key: {file_key!r}")

    path = file_map[file_key]
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")

    name = os.path.basename(path)
    os.remove(path)
    return {"message": "File deleted", "deleted": name}
