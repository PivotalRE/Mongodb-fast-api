from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
import csv
import io
from api.db.connection import get_db 
from api.utils.helpers import REQUIRED_COLUMN_MAPPINGS, OPTIONAL_COLUMN_MAPPINGS

router = APIRouter()

@router.get("/", tags=["System"])
def root():
    return {
        "message": "Welcome to the Unified Real Estate Data API.",
        "version": "1.0.0",
        "docs": "/docs"
    }

@router.get("/upload/requirements/unified", tags=["System"])
def get_requirements():
    return {
        "required_fields": REQUIRED_COLUMN_MAPPINGS,
        "optional_fields": OPTIONAL_COLUMN_MAPPINGS
    }

@router.get("/upload/sessions/{session_id}", tags=["System"])
def get_upload_session(session_id: str, db=Depends(get_db)):
    session = db.upload_sessions.find_one({"upload_id": session_id}, {"_id": 0})
    if not session:
        raise HTTPException(404, detail="Session not found")
    return session

@router.get("/upload/sessions/{session_id}/error_rows.csv", tags=["Reporting"])
def download_error_csv(session_id: str, db=Depends(get_db)):
    session = db.upload_sessions.find_one({"upload_id": session_id})
    if not session or "errors" not in session:
        raise HTTPException(404, "No errors found")
    output = io.StringIO()
    errors = session["errors"][:1000]
    fieldnames = set()
    for e in errors:
        fieldnames.update(e["raw_data"].keys())
    writer = csv.DictWriter(output, fieldnames=sorted(fieldnames))
    writer.writeheader()
    for e in errors:
        writer.writerow({k: v for k, v in e["raw_data"].items()})
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv")
