from fastapi import APIRouter, UploadFile, File, BackgroundTasks, Depends, HTTPException
from fastapi.responses import JSONResponse
from starlette import status
from datetime import datetime, timezone
import uuid
import csv
import io
import logging

# Update the import path if helpers.py is in a different location, for example:
# from api.helpers import normalize_column_name, REQUIRED_COLUMN_MAPPINGS
from api.utils.helpers import normalize_column_name, REQUIRED_COLUMN_MAPPINGS
from api.utils.processing import process_unified_upload

# Import or define api_key_header and get_db dependencies
from api.utils.common import api_key_header

router = APIRouter()

logger = logging.getLogger(__name__)

from fastapi import Request

def get_db(request: Request):
    return request.app.state.db

@router.post("/upload/unified", tags=["Data Ingestion"])
async def upload_unified_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db=Depends(get_db),
    api_key: str = Depends(api_key_header)
):
    session_id = f"UNIFIED_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"
    try:
        content = await file.read()
        csv_file = io.TextIOWrapper(io.BytesIO(content), encoding="utf-8")
        reader = csv.DictReader(csv_file)
        logger.info(f"Raw CSV headers: {reader.fieldnames}")
        logger.info(f"Normalized headers: {[normalize_column_name(col) for col in reader.fieldnames]}")
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="Empty CSV file")
        missing_fields = {}
        received_columns = [normalize_column_name(col) for col in reader.fieldnames]
        logger.info(f"Normalized columns received: {received_columns}")
        for canonical, aliases in REQUIRED_COLUMN_MAPPINGS.items():
            if canonical == "apn":
                found = "apn" in received_columns
            else:
                found = any(
                    normalize_column_name(alias) in received_columns
                    for alias in aliases
                )
            if not found:
                missing_fields[canonical] = {
                    "expected_aliases": aliases,
                    "received_columns": received_columns
                }
        if missing_fields:
            error_details = {
                "error_type": "validation_error",
                "missing_fields": missing_fields,
                "received_columns": reader.fieldnames,
                "suggestion": "See /upload/requirements/unified for acceptable column names"
            }
            return JSONResponse(
                content={
                    "status": "failed",
                    "session_id": session_id,
                    "error": "Missing required fields",
                    "details": error_details,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
            )
        background_tasks.add_task(
            process_unified_upload,
            content,
            session_id,
            db
        )
        return {
            "status": "processing",
            "session_id": session_id,
            "filename": file.filename,
            "message": "Data is being processed in the background"
        }
    except Exception as e:
        logger.error(f"Initial validation failed: {str(e)}")
        raise HTTPException(400, detail=str(e))