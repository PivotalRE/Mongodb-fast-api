import sys
import logging
import os
import uuid
import csv
import io
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader
from starlette import status
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from bson import json_util
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

load_dotenv()
app = FastAPI(title="Unified Real Estate Data API", version="1.0.0")

API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

REQUIRED_COLUMN_MAPPINGS = {
    "apn": ["apn", "parcel id", "parcel number"],
    "first name": ["first name", "owner.first_name", "owner first name", "first", "firstname"],
    "last name": ["last name", "owner.last_name", "owner last name", "last", "lastname"],
    "property address": ["property address", "address.street", "address street", "street"]
}

OPTIONAL_COLUMN_MAPPINGS = {
    # Property
    "property city": ["property city", "address.city"],
    "property state": ["property state", "address.state"],
    "property zip": ["property zip", "address.zip"],
    "bedrooms": ["bedrooms"],
    "bathrooms": ["bathrooms"],
    "sqft": ["sqft"],
    "year": ["year", "year built"],
    "estimated value": ["estimated value"],
    "last sale price": ["last sale price"],
    "last sold": ["last sold"],

    # Mailing
    "mailing address": ["mailing address"],
    "mailing city": ["mailing city"],
    "mailing state": ["mailing state"],
    "mailing zip": ["mailing zip", "mailing zip5"],

    # Owner & contact
    "status": ["status"],
    "tags": ["tags"],

    # Emails
    "email 1": [f"email {i}" for i in range(1, 11)],

    # Phone support already dynamically handled in your loop

    # Life events
    "tax delinquent year": ["tax delinquent year"],
    "tax delinquent value": ["tax delinquent value"]
}



def get_db():
    return app.state.db

# ------------------- Helpers -------------------

def normalize_column_name(col: str) -> str:
    normalized = (
        col.strip().lower()
        .replace('_', ' ').replace('.', ' ').replace('-', ' ')
    )
    # Collapse extra spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    # Add space between word and number (e.g., 'phone1' → 'phone 1')
    normalized = re.sub(r'([a-zA-Z])(\d+)', r'\1 \2', normalized)
    
    # Special case aliases
    normalized = normalized.replace("address street", "property address")
    normalized = normalized.replace("owner first name", "first name")
    normalized = normalized.replace("owner last name", "last name")
    return normalized


def clean_apn(raw_apn: str) -> Optional[str]:
    """More robust APN cleaning with better validation"""
    if not raw_apn:
        return None
        return None
        
    # Remove all non-alphanumeric chars except hyphens
    cleaned = re.sub(r"[^\w-]", "", str(raw_apn).upper().strip())
    
    # Handle common placeholder values
    if cleaned in ["N/A", "NA", "NULL", "MISSING"]:
        return None
        
    # Pad numeric APNs with leading zeros if needed
    if cleaned.isdigit():
        if len(cleaned) < 5:
            return None  # Reject clearly invalid numeric APNs
        if 5 <= len(cleaned) < 12:
            return cleaned.zfill(12)  # Standardize to 12 digits
            
    return cleaned if len(cleaned) >= 5 else None

def clean_phone(phone: str) -> Optional[str]:
    if not phone:
        return None
    cleaned = re.sub(r"[^0-9]", "", str(phone).strip())
    if len(cleaned) == 10:
        return cleaned
    elif len(cleaned) == 11 and cleaned.startswith("1"):
        return cleaned[1:]  # Strip US country code
    logger.warning(f"Invalid phone format: {phone}")
    return None
def validate_zip(zip_code: str) -> Optional[str]:
    cleaned = re.sub(r"[^0-9]", "", str(zip_code).strip())
    return cleaned if len(cleaned) in [5, 9] else None

def safe_int(value: Any) -> Optional[int]:
    try: return int(float(value)) if value not in [None, ""] else None
    except: return None

def safe_float(value: Any) -> Optional[float]:
    try: return float(value) if value not in [None, ""] else None
    except: return None

def parse_array(value: str) -> List[str]:
    if not value: return []
    return [v.strip().strip('"') for v in re.split(r'[|,;]', str(value)) if v.strip()]

def map_column(column_name: str, mappings: Dict[str, List[str]]) -> Optional[str]:
    normalized = normalize_column_name(column_name)
    for canonical, aliases in mappings.items():
        if any(normalize_column_name(alias) == normalized for alias in aliases):
            return canonical
    return None

def generate_owner_hash(first_name: str, last_name: str, mailing_address: str, zip_code: str) -> str:
    raw = f"{first_name.lower().strip()}_{last_name.lower().strip()}_{mailing_address.lower().strip()}_{zip_code}"
    return hashlib.sha256(raw.encode()).hexdigest()

# ------------------- Row Processing -------------------
def process_unified_row(row: Dict) -> Optional[Dict]:
    try:
        processed_row = {
            normalize_column_name(k): str(v).strip() if v is not None else ""
            for k, v in row.items()
        }

        mapped_row = {}
        for col_name, value in processed_row.items():
            canonical = map_column(col_name, REQUIRED_COLUMN_MAPPINGS) or \
                       map_column(col_name, OPTIONAL_COLUMN_MAPPINGS)
            if canonical:
                mapped_row[canonical] = value

        apn = clean_apn(mapped_row.get("apn", ""))
        
        
        if not apn:
            error_msg = f"Missing or invalid APN: raw = '{mapped_row.get('apn', '')}'"
            logger.warning(f"Skipping row with missing/invalid APN: raw = '{mapped_row.get('apn', '')}'")
            return {
                "error": {
                    "error_type": "validation_error",
                    "message": error_msg,
                    "raw_data": row
                }
            }


        if mapped_row.get("property state", "").strip().upper() != "WA":
            logger.info(f"Skipping non-WA property: {mapped_row.get('property state')}")
            return None

        normalized_owner_id = generate_owner_hash(
            mapped_row.get("first name", ""),
            mapped_row.get("last name", ""),
            mapped_row.get("mailing address", ""),
            mapped_row.get("mailing zip", "")
        )
        owner_id = f"OWN-{normalized_owner_id[:8]}"

        property_doc = {
            "apn": apn,
            "address": {
                "street": mapped_row.get("property address", ""),
                "city": mapped_row.get("property city", ""),
                "state": mapped_row.get("property state", "").upper()[:2],
                "zip": validate_zip(mapped_row.get("property zip", ""))
            },
            "characteristics": {
                "bedrooms": safe_int(mapped_row.get("bedrooms", "")),
                "bathrooms": safe_float(mapped_row.get("bathrooms", "")),
                "sqft": safe_float(mapped_row.get("sqft", "")),
                "year_built": safe_int(mapped_row.get("year", ""))
            },
            "valuation": {
                "estimated_value": safe_float(mapped_row.get("estimated value", "")),
                "last_sale_price": safe_float(mapped_row.get("last sale price", ""))
            },
            "last_updated": datetime.now(timezone.utc)
        }
        emails = []
        for i in range(1, 11):
            email = processed_row.get(f"email {i}", "").strip()
            if email and re.match(r"[^@]+@[^@]+\.[^@]+", email):
                emails.append(email.lower())
            elif email:
                logger.warning(f"Invalid email format: {email}")

        owner_doc = {
            "owner_id": owner_id,
            "normalized_owner_id": normalized_owner_id,
            "apn": apn,
            "full_name": f"{mapped_row.get('first name', '')} {mapped_row.get('last name', '')}".strip(),
            "mailing_address": {
                "street": mapped_row.get("mailing address", ""),
                "city": mapped_row.get("mailing city", ""),
                "state": mapped_row.get("mailing state", "").upper()[:2],
                "zip": validate_zip(mapped_row.get("mailing zip", ""))
            },
            "emails": emails,
            "phone_ids": [],
            "tags": parse_array(mapped_row.get("tags", "")),
            "status": mapped_row.get("status", "unknown").lower(),
            "last_updated": datetime.now(timezone.utc)
        }

        phone_docs = []
        for i in range(1, 31):
            phone = mapped_row.get(f"phone {i}", "")
            if not phone:
                continue
            phone_number = clean_phone(phone)
            if phone_number:
                logger.info(f"[Phone Found] {phone_number} from column: phone {i}")
                # Generate deterministic phone_id based on the phone number
                phone_id_hash = hashlib.sha256(phone_number.encode()).hexdigest()[:8]
                phone_id = f"PHONE-{phone_id_hash}"
                phone_docs.append({
                    "phone_id": phone_id,
                    "number": phone_number,
                    "type": mapped_row.get(f"phone type {i}", "UNKNOWN").upper(),
                    "status": mapped_row.get(f"phone status {i}", "UNVERIFIED").upper(),
                    "tags": parse_array(mapped_row.get(f"phone tags {i}", "")),
                    "linked_apns": [apn],
                    "linked_owners": [owner_id],
                    "last_updated": datetime.now(timezone.utc)
                })
                owner_doc["phone_ids"].append(phone_id)


        life_events = []
        tax_year = mapped_row.get("tax delinquent year", "")
        tax_value = mapped_row.get("tax delinquent value", "")
        if tax_year and tax_value:
            try:
                life_events.append({
                    "event_type": "TAX_DELINQUENCY",
                    "date": datetime(int(tax_year), 1, 1),
                    "amount": safe_float(tax_value),
                    "description": f"Tax delinquent since {tax_year}",
                    "last_updated": datetime.now(timezone.utc)
                })
            except ValueError:
                pass

        last_sold = mapped_row.get("last sold", "")
        sale_price = mapped_row.get("last sale price", "")
        if last_sold and sale_price:
            try:
                sale_date = datetime.strptime(last_sold, "%Y-%m-%d %H:%M:%S")
                property_doc["sale_history"] = [{
                    "event_type": "SALE",
                    "date": sale_date,
                    "amount": safe_float(sale_price),
                    "description": "Property sale recorded",
                    "last_updated": datetime.now(timezone.utc)
                }]
                property_doc["last_sale"] = {
                    "date": sale_date,
                    "price": safe_float(sale_price)
                }
                # In process_unified_row()
                logger.debug(f"Processing row with APN: {apn}")
                logger.debug(f"Found emails: {emails}")
                logger.debug(f"Found phones: {[p['number'] for p in phone_docs]}")
            except Exception as e:
                logger.warning(f"Failed to parse last_sold date: {last_sold} — {str(e)}")


        return {
            "property": property_doc,
            "owner": owner_doc,
            "phones": phone_docs,
            "life_events": life_events
        }

    except Exception as e:
        logger.error(f"Row processing error: {str(e)}")
        return None

def process_unified_batch(batch: List[Dict], db):
    """Process a batch of rows"""
    prop_ops = []
    owner_ops = []
    phone_ops = []
    life_event_ops = []
    errors = []  # Initialize errors as an empty list
    
    for row in batch:
        try:
            entities = process_unified_row(row)

            if not entities:
                continue

            if "error" in entities:
                errors.append({
                    "row": row.get("row_number", -1),
                    "error_type": entities["error"]["error_type"],
                    "message": entities["error"]["message"],
                    "raw_data": entities["error"]["raw_data"]
                })
                continue

            # Now safe to access: entities["property"], entities["owner"], etc.
            prop_ops.append(UpdateOne(
                {"apn": entities["property"]["apn"]},
                {"$set": entities["property"]},
                upsert=True
            ))

            
            # Owner insert
            owner_update = {
                "$setOnInsert": {
                    "normalized_owner_id": entities["owner"]["normalized_owner_id"],
                    "apn": entities["owner"]["apn"],
                    "full_name": entities["owner"]["full_name"],
                    "mailing_address": entities["owner"]["mailing_address"]
                },
                "$addToSet": {
                    "emails": {"$each": entities["owner"]["emails"]},  # Add this line
                    "phone_ids": {"$each": entities["owner"]["phone_ids"]},
                    "tags": {"$each": entities["owner"]["tags"]}
                },
                "$set": {
                    "status": entities["owner"]["status"],
                    "last_updated": entities["owner"]["last_updated"]
                }
            }

            # Only include $addToSet if phone_ids is not empty to avoid MongoDB write conflict
            if entities["owner"]["phone_ids"]:
                owner_update["$addToSet"] = {
                    "phone_ids": {"$each": entities["owner"]["phone_ids"]}
                }

            owner_ops.append(UpdateOne(
                {"normalized_owner_id": entities["owner"]["normalized_owner_id"]},
                owner_update,
                upsert=True
            ))
            
            
            if not entities:
                continue

            if "error" in entities:
                errors.append({
                    "row": row.get("row_number", -1),
                    "error_type": entities["error"]["error_type"],
                    "message": entities["error"]["message"],
                    "raw_data": entities["error"]["raw_data"]
                })
                continue



            # Phone updates
            for phone in entities["phones"]:
                logger.info(f"[Mongo Upsert] Inserting phone: {phone['number']} (ID: {phone['phone_id']})")
                phone_ops.append(UpdateOne(
                    {"number": phone["number"]},
                    {
                        "$setOnInsert": {
                            "phone_id": phone["phone_id"],
                            "number": phone["number"],
                            "type": phone["type"],
                            "status": phone["status"],
                            "tags": phone.get("tags", []),
                            "linked_apns": phone["linked_apns"],
                            "linked_owners": phone["linked_owners"],
                            "last_updated": phone["last_updated"]
                        },
                        "$addToSet": {
                            "linked_apns": {"$each": phone["linked_apns"]},
                            "linked_owners": {"$each": phone["linked_owners"]}
                        },
                        "$set": {
                            "last_updated": phone["last_updated"]
                        }
                    },
                    upsert=True
                ))


            
            # Life events
            for event in entities["life_events"]:
                if event["event_type"] == "SALE":
                    continue 
                life_event_ops.append(UpdateOne(
                    {"apn": entities["property"]["apn"], "event_type": event["event_type"]},
                    {"$set": event},
                    upsert=True
                ))
                
        except Exception as e:
            logger.error(f"Batch processing error: {str(e)}")
    
    # Execute bulk writes
    results = {}
    try:
        if prop_ops:
            results["properties"] = db.properties.bulk_write(prop_ops).bulk_api_result
        if owner_ops:
            results["owners"] = db.owners.bulk_write(owner_ops).bulk_api_result
        if phone_ops:
            results["phones"] = db.phones.bulk_write(phone_ops).bulk_api_result
        if life_event_ops:
            results["life_events"] = db.life_events.bulk_write(life_event_ops).bulk_api_result
    except BulkWriteError as bwe:
        logger.error(f"Bulk write error: {bwe.details}")
        results["errors"] = bwe.details
    
    return results

# --- API Endpoints ---
@app.post("/upload/unified", tags=["Data Ingestion"])
async def upload_unified_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db=Depends(get_db),
    api_key: str = Depends(api_key_header)
):
    """Upload unified CSV file"""
    session_id = f"UNIFIED_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"
    
    try:
        content = await file.read()
        csv_file = io.TextIOWrapper(io.BytesIO(content), encoding="utf-8")
        reader = csv.DictReader(csv_file)
        # In the upload_unified_csv endpoint, after reader = csv.DictReader(csv_file)
        logger.info(f"Raw CSV headers: {reader.fieldnames}")
        logger.info(f"Normalized headers: {[normalize_column_name(col) for col in reader.fieldnames]}")
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="Empty CSV file")
            
        # Validate required columns
        missing_fields = {}
        received_columns = [normalize_column_name(col) for col in reader.fieldnames]
        logger.info(f"Normalized columns received: {received_columns}")

        for canonical, aliases in REQUIRED_COLUMN_MAPPINGS.items():
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
def process_unified_upload(file_bytes: bytes, session_id: str, db):
    """Background processing task for unified CSV uploads"""
    batch_size = 1000
    processed = 0
    errors = [] 
    
    try:
        csv_file = io.TextIOWrapper(io.BytesIO(file_bytes), encoding="utf-8")
        reader = csv.DictReader(csv_file)
        
        # Initialize session tracking
        db.upload_sessions.update_one(
            {"upload_id": session_id},
            {"$set": {
                "upload_id": session_id,
                "collection": "unified",
                "status": "processing",
                "start_time": datetime.now(timezone.utc),
                "processed_count": 0,
                "error_count": 0,
                "errors": []
            }},
            upsert=True
        )

        # Process in batches
        batch = []
        for row_num, row in enumerate(reader, 1):
            try:
                normalized_row = {
                    "row_number": row_num,  # Add row number to context
                    "data": {
                        normalize_column_name(k): str(v).strip() 
                        for k, v in row.items()
                    }
                }
                batch.append(normalized_row)
                
                # Process batch when size reached
                if len(batch) >= batch_size:
                    result = process_unified_batch(batch, db)
                    processed += len(batch)
                    db.upload_sessions.update_one(
                        {"upload_id": session_id},
                        {"$inc": {
                            "processed_count": len(batch),
                            "error_count": len(result.get("errors", []))
                        }}
                    )
                    batch = []
                    
            except Exception as e:
                errors.append({
                    "row": row_num,
                    "error_type": "processing_error",
                    "message": str(e),
                    "raw_data": {k: v for k, v in row.items() if k.lower() not in ['password', 'ssn']}
                })


        # Process final batch
        if batch:
            process_unified_batch(batch, db)
            processed += len(batch)

        # Mark session as completed
        db.upload_sessions.update_one(
            {"upload_id": session_id},
            {"$set": {
                "status": "completed",
                "end_time": datetime.now(timezone.utc),
                "processed_count": processed,
                "error_count": len(errors),
                "errors": errors[:1000]  # Store first 1000 errors
            }}
        )

    except Exception as e:
        logger.error(f"[{session_id}] Processing failed: {str(e)}")
        db.upload_sessions.update_one(
            {"upload_id": session_id},
            {"$set": {
                "status": "failed",
                "error_message": str(e),
                "end_time": datetime.now(timezone.utc),
                "processed_count": processed,
                "error_count": len(errors),
                "errors": errors[:1000]
            }}
        )
    finally:
        # No need to close client here - connection is managed by app lifecycle
        pass  
@app.get("/properties/{apn}", tags=["Query"])
def get_property(apn: str, db=Depends(get_db)):
    """Get full property details with relationships"""
    pipeline = [
        {"$match": {"apn": clean_apn(apn)}},
        {"$lookup": {
            "from": "owners",
            "localField": "apn",
            "foreignField": "apn",
            "as": "owners"
        }},
        {"$lookup": {
            "from": "life_events",
            "localField": "apn",
            "foreignField": "apn",
            "as": "life_events"
        }},
        {"$lookup": {
            "from": "phones",
            "let": {"owner_phones": "$owners.phone_ids"},
            "pipeline": [
                {"$match": {"$expr": {"$in": ["$phone_id", "$$owner_phones"]}}}
            ],
            "as": "phones"
        }},
        {"$project": {
            "_id": 0,
            "property": "$$ROOT",
            "owners": 1,
            "life_events": 1,
            "phones": 1
        }}
    ]
    
    result = list(db.properties.aggregate(pipeline))
    return json.loads(json_util.dumps(result))

@app.get("/upload/sessions/{session_id}", tags=["System"])
def get_upload_session(session_id: str, db=Depends(get_db)):
    """Get upload session status"""
    session = db.upload_sessions.find_one(
        {"upload_id": session_id},
        {"_id": 0}
    )
    if not session:
        raise HTTPException(404, detail="Session not found")
    
    # Convert datetime to ISO string
    if 'timestamp' in session:
        session['timestamp'] = session['timestamp'].isoformat()
    
    return session

@app.get("/upload/requirements/unified", tags=["System"])
async def get_unified_requirements():
    """Get CSV column requirements"""
    return {
        "required_fields": REQUIRED_COLUMN_MAPPINGS,
        "optional_fields": OPTIONAL_COLUMN_MAPPINGS,
        "notes": [
            "Column names are case-insensitive",
            "Nested fields can use dot notation (e.g., 'address.street')",
            "Special characters are ignored during matching"
        ]
    }


@app.get("/upload/sessions/{session_id}/report", tags=["Reporting"])
def get_upload_summary_report(session_id: str, format: str = "json", db=Depends(get_db)):
    """Download a summary report of a processed upload"""
    session = db.upload_sessions.find_one({"upload_id": session_id})
    if not session:
        raise HTTPException(status_code=404, detail="Upload session not found")

    report = {
        "upload_id": session.get("upload_id"),
        "collection": session.get("collection"),
        "status": session.get("status"),
        "start_time": session.get("start_time").isoformat() if session.get("start_time") else None,
        "end_time": session.get("end_time").isoformat() if session.get("end_time") else None,
        "processed_count": session.get("processed_count", 0),
        "error_count": session.get("error_count", 0),
        "errors_summary": {}
    }

    for err in session.get("errors", []):
        key = err.get("error_type", "unknown")
        report["errors_summary"].setdefault(key, 0)
        report["errors_summary"][key] += 1

    if format == "csv":
        from fastapi.responses import StreamingResponse
        import csv
        import io

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=report.keys())
        writer.writeheader()
        writer.writerow(report)
        output.seek(0)
        return StreamingResponse(output, media_type="text/csv")

    return report


@app.get("/upload/sessions/{session_id}/errors.csv", tags=["Reporting"])
def download_errors_csv(session_id: str, db=Depends(get_db)):
    session = db.upload_sessions.find_one({"upload_id": session_id})
    if not session or "errors" not in session:
        raise HTTPException(status_code=404, detail="No errors for this session")

    import io, csv
    output = io.StringIO()
    keys = ["row", "error_type", "message"]
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    for err in session["errors"]:
        writer.writerow({
            "row": err.get("row"),
            "error_type": err.get("error_type", "unknown"),
            "message": err.get("message", str(err.get("error", "")))
        })
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv")

# --- Index Management ---
@app.on_event("startup")
async def startup_db_client():
    """Initialize MongoDB connection and create indexes"""
    try:
        # Create persistent client
        app.state.mongo_client = MongoClient(os.getenv("MONGO_URI"))
        app.state.db = app.state.mongo_client[os.getenv("DB_NAME", "RealEstate")]
        
        # Create indexes
        db = app.state.db
        if "upload_sessions" not in db.list_collection_names():
            db.create_collection("upload_sessions")
        
        db.properties.create_index([("apn", 1)], unique=True, background=True)
        db.properties.create_index([("address.zip", 1)], background=True)
        db.owners.create_index([("apn", 1)], background=True)
        db.owners.create_index([("phone_ids", 1)], background=True)
        db.phones.create_index([("number", 1)], unique=True, background=True)
        db.phones.create_index([("linked_apns", 1)], background=True)
        db.life_events.create_index([("apn", 1)], background=True)

        logger.info("Database connection established and indexes verified")
        
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_db_client():
    """Close MongoDB connection on shutdown"""
    if hasattr(app.state, "mongo_client"):
        app.state.mongo_client.close()
        logger.info("MongoDB connection closed")

def get_db():
    """Dependency to get database instance"""
    return app.state.db

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)