import sys
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks
from fastapi.security import APIKeyHeader
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from typing import Optional, List, Dict
import os
import uuid
import csv
import io
from datetime import datetime, timezone
from dotenv import load_dotenv
from bson import json_util
import json
from bson import ObjectId

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from import_data import (
    clean_apn,
    COLLECTION_CONFIG,
    build_query,
    process_properties,
    process_owners,
    process_phones,
    process_life_events 
)

# Initialize logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()
app = FastAPI(title="Pivotal Real Estate API",
             version="1.0.0",
             description="API for managing real estate data and automation")

# Security
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

def get_api_key(api_key: str = Depends(api_key_header)):
    if api_key != os.getenv("API_SECRET_KEY"):
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return api_key

# Database setup
def get_db():
    client = MongoClient(os.getenv("MONGO_URI"))
    try:
        db = client[os.getenv("DB_NAME", "PivotalRealEstate")]
        yield db
    finally:
        client.close()

# --- Core Endpoints ---

@app.post("/upload/{collection_name}",
         response_model=Dict,
         tags=["Data Ingestion"])
async def upload_csv(
    collection_name: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db=Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Upload CSV to specified collection (properties|owners|phones)
    """
    allowed_collections = ["properties", "owners", "phones", "life_events"]
    if collection_name not in allowed_collections:
        raise HTTPException(400, detail="Invalid collection name")

    session_id = f"API_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"
    
    background_tasks.add_task(
        process_upload,
        collection_name,
        await file.read(),
        session_id,
        db
    )
    
    return {
        "status": "processing",
        "session_id": session_id,
        "collection": collection_name,
        "filename": file.filename
    }

def process_upload(collection_name: str, file_bytes: bytes, session_id: str, _):
    """Robust background task for CSV processing with debug logs and error handling"""
    # Create a fresh MongoDB connection for background task
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME", "PivotalRealEstate")]

    try:
        logger.info(f"[{session_id}] Starting upload for: {collection_name}")
        
        processor_map = {
            "properties": process_properties,
            "owners": process_owners,
            "phones": process_phones,
            "life_events": process_life_events
        }
        
        processor = processor_map.get(collection_name)
        if not processor:
            raise ValueError(f"No processor found for collection: {collection_name}")

        csv_content = file_bytes.decode('utf-8').strip()
        debug_path = f"/tmp/debug_{collection_name}_{session_id}.csv"
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(csv_content)
        logger.info(f"[{session_id}] Backup written to {debug_path}")

        if not csv_content:
            raise ValueError("Uploaded CSV is empty")
        
        csv_file = io.StringIO(csv_content)
        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError("CSV header row is missing")

        logger.info(f"[{session_id}] CSV headers: {reader.fieldnames}")

        processed = []
        error_count = 0
        for row_num, row in enumerate(reader, start=1):
            try:
                logger.debug(f"[{session_id}] Row {row_num}: {row}")
                processed_record = processor(row)
                if processed_record:
                    processed.append(processed_record)
                if row_num % 100 == 0:
                    logger.info(f"[{session_id}] Processed {row_num} rows...")
            except Exception as e:
                error_count += 1
                logger.warning(f"[{session_id}] Error at row {row_num}: {e}")
                logger.debug(f"[{session_id}] Problematic row: {row}")

        logger.info(f"[{session_id}] Total valid rows: {len(processed)}")

        collection = db[collection_name]
        bulk_ops = []
        for record in processed:
            try:
                query = build_query(record, COLLECTION_CONFIG[collection_name]["dedup_keys"])
                if query:
                    bulk_ops.append(UpdateOne(query, {"$set": record}, upsert=True))
            except Exception as e:
                logger.warning(f"[{session_id}] Skipping record due to query error: {e}")

        result = None
        if bulk_ops:
            try:
                result = collection.bulk_write(bulk_ops, ordered=False)
                logger.info(f"[{session_id}] Mongo result: {result.bulk_api_result}")
            except BulkWriteError as bwe:
                logger.error(f"[{session_id}] Bulk write error: {bwe.details}")
                error_count += len(bwe.details.get("writeErrors", []))

        session_data = {
            "upload_id": session_id,
            "collection": collection_name,
            "status": "completed" if error_count == 0 else "completed_with_errors",
            "record_count": len(processed),
            "error_count": error_count,
            "inserted": result.inserted_count if result else 0,
            "updated": result.modified_count if result else 0,
            "timestamp": datetime.now(timezone.utc),
            "file_headers": reader.fieldnames
        }

        db.upload_sessions.insert_one(session_data)
        logger.info(f"[{session_id}] Upload session saved with {len(processed)} records")

    except Exception as e:
        logger.error(f"[{session_id}] Critical failure: {e}")
        db.upload_sessions.insert_one({
            "upload_id": session_id,
            "collection": collection_name,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        })

    finally:
        client.close()

# --- Query Endpoints ---

@app.get("/upload_sessions/{session_id}", response_model=Dict, tags=["System"])
def get_upload_session(session_id: str, db=Depends(get_db)):
    """Get detailed upload session status"""
    session = db.upload_sessions.find_one(
        {"upload_id": session_id},
        {'_id': 0}
    )
    if not session:
        raise HTTPException(404, detail="Session not found")
    
    # Convert BSON to JSON
    return json.loads(json_util.dumps(session))

@app.get("/owners", response_model=List[Dict], tags=["Query"])
def get_owners(
    apn: Optional[str] = None,
    name: Optional[str] = None,
    zip_code: Optional[str] = None,
    db=Depends(get_db)
):
    """Search owners and join with their phones"""
    pipeline = []

    match = {}
    if apn:
        match["apn"] = clean_apn(apn)
    if name:
        match["$or"] = [
            {"full_name": {"$regex": name, "$options": "i"}},
            {"first_name": {"$regex": name, "$options": "i"}},
            {"last_name": {"$regex": name, "$options": "i"}}
        ]
    if zip_code:
        match["mailing_zip"] = zip_code
    if match:
        pipeline.append({"$match": match})

    pipeline.append({
        "$lookup": {
            "from": "phones",
            "localField": "apn",
            "foreignField": "owner_apn",
            "as": "phones"
        }
    })

    pipeline.append({ "$project": { "_id": 0 } })  # <-- Exclude _id
    pipeline.append({ "$limit": 100 })

    return list(db.owners.aggregate(pipeline))

@app.get("/properties", response_model=List[Dict], tags=["Query"])
def get_properties(
    apn: Optional[str] = None,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
    db=Depends(get_db)
):
    """Search properties by APN, state, or zip"""
    query = {}
    if apn:
        query["apn"] = clean_apn(apn)
    if state:
        query["address.state"] = state.upper()
    if zip_code:
        query["address.zip"] = zip_code

    return list(db.properties.find(query, {'_id': 0}).limit(100))


@app.post("/enrich/phone", tags=["Enrichment"])
async def enrich_phone(
    phone_number: str,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key)
):
    """Trigger phone number enrichment logic (placeholder)"""
    from import_data import clean_phone  # import here to avoid circular issue
    clean_num = clean_phone(phone_number)
    if not clean_num:
        raise HTTPException(400, detail="Invalid phone number")

    background_tasks.add_task(run_phone_enrichment, clean_num)
    return {"status": "enrichment_started", "phone": clean_num}


def run_phone_enrichment(phone_number: str):
    """Placeholder background enrichment task"""
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME", "PivotalRealEstate")]
    logger.info(f"Enriching phone: {phone_number}")
    db.phones.update_one(
        {"number": phone_number},
        {"$set": {
            "status": "enriched",
            "last_updated": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    client.close()


@app.get("/health", tags=["System"])
def health_check(db=Depends(get_db)):
    """System health check"""
    try:
        db.command('ping')
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}
    

@app.get("/")
def root():
    return {"message": "App is running!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")