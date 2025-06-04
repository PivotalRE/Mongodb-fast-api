import os
import sys
import csv
import io
import re
import json
import math
import uuid
import hashlib
import logging
import time
import random
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader
from starlette import status
from fastapi.responses import FileResponse

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError
import pymongo

from bson import ObjectId, json_util
from dotenv import load_dotenv

from rapidfuzz import fuzz # type: ignore
from dateutil.parser import parse
from email_validator import validate_email, EmailNotValidError # type: ignore
import requests
import phonenumbers     # type: ignore
import tempfile
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scrapers.selenium_google import get_parcel_number
from scrapers.kingCounty_Scraper import scrape_king_county_properties
from scrapers.kingCounty_Scraper import scrape_from_mongo_and_update  # ensure it's implemented as shown before

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
    "apn": ["apn"],
    "first name": ["first name", "owner.first_name", "owner first name", "first", "firstname"],
    "last name": ["last name", "owner.last_name", "owner last name", "last", "lastname"],
    "property address": ["property address", "address.street", "address street", "street"]
}

OPTIONAL_COLUMN_MAPPINGS = {
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
    "mailing address": ["mailing address"],
    "mailing city": ["mailing city"],
    "mailing state": ["mailing state"],
    "mailing zip": ["mailing zip", "mailing zip5"],
    "status": ["status"],
    "tags": ["tags"],
    "email 1": [f"email {i}" for i in range(1, 11)],
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
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = re.sub(r'([a-zA-Z])(\d+)', r'\1 \2', normalized)
    normalized = normalized.replace("address street", "property address")
    normalized = normalized.replace("owner first name", "first name")
    normalized = normalized.replace("owner last name", "last name")
    return normalized

def clean_apn(raw_apn: str) -> Optional[str]:
    cleaned = re.sub(r"[^\d]", "", str(raw_apn).strip())
    if not cleaned.isdigit():
        return None
    return cleaned.zfill(10)

def is_invalid_apn(apn_val):
    if apn_val is None:
        return True
    if isinstance(apn_val, float) and math.isnan(apn_val):
        return True
    apn_str = str(apn_val).strip().lower()
    return apn_str in {'', 'n/a', 'none', 'nan'}

def clean_phone(phone: str) -> Optional[str]:
    try:
        parsed = phonenumbers.parse(phone, "US")
        if phonenumbers.is_valid_number(parsed):
            formatted = phonenumbers.format_number(
                parsed, 
                phonenumbers.PhoneNumberFormat.E164
            ).replace("+1", "")
            logger.debug(f"Cleaned phone: {phone} → {formatted}")
            return formatted
        else:
            logger.warning(f"Invalid phone number (not valid): {phone}")
            return None
    except phonenumbers.NumberParseException as e:
        logger.warning(f"Failed to parse phone {phone}: {str(e)}")
        return None

def validate_zip(zip_code: str) -> Optional[str]:
    cleaned = re.sub(r"[^0-9]", "", str(zip_code).strip())
    return cleaned if len(cleaned) == 5 else None

def extract_best_zip(row: Dict[str, str], zip_keys: List[str]) -> Optional[str]:
    for key in zip_keys:
        zip_val = row.get(key)
        if not zip_val:
            continue
        direct_clean = validate_zip(zip_val)
        if direct_clean:
            return direct_clean
        if "-" in zip_val:
            base_zip = zip_val.split("-")[0]
            if validate_zip(base_zip):
                return base_zip
    return None

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
    if normalized in mappings:
        return normalized
    for canonical, aliases in mappings.items():
        if any(normalize_column_name(alias) == normalized for alias in aliases):
            return canonical
    return None

def generate_owner_hash(first_name: str, last_name: str, mailing_address: str, zip_code: str) -> str:
    raw = f"{first_name.lower().strip()}_{last_name.lower().strip()}_{mailing_address.lower().strip()}_{zip_code}"
    return hashlib.sha256(raw.encode()).hexdigest()

def find_best_db_match(address, name, db):
    candidates = db.properties.find({"address.street": {"$exists": True}})
    best_match = None
    best_score = 0
    for prop in candidates:
        addr_similarity = fuzz.ratio(address, prop["address"].get("street", ""))
        owner_full_name = ""
        if prop.get("owner") and "full_name" in prop["owner"]:
            owner_full_name = prop["owner"]["full_name"]
        name_similarity = fuzz.ratio(name, owner_full_name)
        total_score = (addr_similarity * 0.8) + (name_similarity * 0.2)
        if total_score > best_score:
            best_score = total_score
            best_match = prop
    return best_match, best_score

def standardize_address(raw_address: str, city: str, state: str, zip_code: str) -> Dict[str, str]:
    street = re.sub(r'\s+', ' ', raw_address.strip().upper())
    city = city.strip().upper() if city else ''
    state = state.strip().upper()[:2] if state else ''
    zip_code = validate_zip(zip_code) or ''
    return {
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_code
    }

def apify_general_scrape(address: str, apify_token: str) -> Optional[str]:
    try:
        payload = {
            "startUrls": [{"url": f"https://www.google.com/search?q={urllib.parse.quote_plus(address + ' parcel number')}"}],
            "maxDepth": 2,
            "maxPagesPerCrawl": 5
        }
        response = requests.post(
            "https://api.apify.com/v2/acts/apify~web-scraper/run-sync-get-dataset-items",
            params={"token": apify_token},
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        data = response.json()
        for item in data:
            text = item.get("text", "")
            apn_match = re.search(r'\b\d{10}\b|\b\d{3}-\d{3}-\d{3}\b', text)
            if apn_match:
                apn = apn_match.group(0).replace("-", "")
                logger.info(f"Apify found APN: {apn}")
                return apn
        logger.warning(f"No APN found in Apify scrape for {address}")
        return None
    except requests.RequestException as e:
        logger.error(f"Apify scrape failed for {address}: {str(e)}")
        return None

def move_out_of_fallback(apn: str, raw_data: Dict, db, candidate_id):
    raw_data["apn"] = apn
    processed = process_unified_row(raw_data, db)
    if processed:
        process_unified_batch([{"row_number": -1, "data": raw_data}], db)
        db.fallback_candidates.delete_one({"_id": candidate_id})

def success_result(_id, apn, confidence, method):
    return {
        "id": str(_id),
        "status": "enriched",
        "apn": apn,
        "confidence": confidence,
        "method": method
    }

# ------------------- Row Processing -------------------

def process_unified_row(row: Dict, db) -> Optional[Dict]:
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
        raw_apn = mapped_row.get("apn", "")
        apn = clean_apn(raw_apn)
        if not raw_apn or not raw_apn.strip():
            db.fallback_candidates.insert_one({
                "raw_data": row,
                "reason": "missing_apn",
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "source": "unified_csv"
            })
            return None
        if not apn:
            db.fallback_candidates.insert_one({
                "raw_data": row,
                "reason": "apn_not_numeric_or_too_short",
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "source": "unified_csv"
            })
            return None
        property_zip = extract_best_zip(
            processed_row, 
            ["property zip 5", "property zip"]
        )
        if not property_zip:
            db.fallback_candidates.insert_one({
                "raw_data": row,
                "reason": "invalid_property_zip",
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "source": "unified_csv"
            })
            return None
        mailing_zip = extract_best_zip(
            processed_row,
            ["mailing zip 5", "mailing zip"]
        )
        if not mailing_zip:
            db.fallback_candidates.insert_one({
                "raw_data": row,
                "reason": "invalid_mailing_zip",
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "source": "unified_csv"
            })
            return None
        if mapped_row.get("property state", "").strip().upper() != "WA":
            logger.info(f"Skipping non-WA property: {mapped_row.get('property state')}")
            return None
        normalized_owner_id = generate_owner_hash(
            mapped_row.get("first name", ""),
            mapped_row.get("last name", ""),
            mapped_row.get("mailing address", ""),
            mailing_zip
        )
        owner_id = f"OWN-{normalized_owner_id[:8]}"
        property_doc = {
            "apn": apn,
            "address": {
                "street": mapped_row.get("property address", ""),
                "city": mapped_row.get("property city", ""),
                "state": mapped_row.get("property state", "").upper()[:2],
                "zip": property_zip
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
            if email:
                try:
                    parsed = validate_email(email, check_deliverability=False)
                    emails.append(parsed.normalized)
                except EmailNotValidError:
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
                "zip": mailing_zip
            },
            "emails": emails,
            "phone_ids": [],
            "tags": parse_array(mapped_row.get("tags", "")),
            "status": mapped_row.get("status", "unknown").lower(),
            "last_updated": datetime.now(timezone.utc)
        }
        phone_docs = []
        for i in range(1, 31):
            phone = processed_row.get(f"phone {i}", "")
            if not phone:
                continue
            phone_number = clean_phone(phone)
            if phone_number:
                phone_id_hash = hashlib.sha256(phone_number.encode()).hexdigest()[:8]
                phone_id = f"PHONE-{phone_id_hash}"
                phone_doc = {
                    "phone_id": phone_id,
                    "number": str(phone_number),
                    "linked_apns": [apn],
                    "linked_owners": [owner_id],
                    "type": mapped_row.get(f"phone type {i}", "UNKNOWN").upper(),
                    "status": mapped_row.get(f"phone status {i}", "UNVERIFIED").upper(),
                    "tags": parse_array(mapped_row.get(f"phone tags {i}", "")),
                    "last_updated": datetime.now(timezone.utc)
                }
                phone_docs.append(phone_doc)
                owner_doc["phone_ids"].append(phone_id)
        known_life_event_fields = {
            "tax auction date": "TAX_AUCTION",
            "tax delinquent value": "TAX_DELINQUENCY",
            "tax delinquent year": "TAX_DELINQUENCY",
            "year behind on taxes": "TAX_DELINQUENCY",
            "lien type": "LIEN",
            "lien recording date": "LIEN",
            "foreclosure date": "FORECLOSURE",
            "bankruptcy recording date": "BANKRUPTCY",
            "divorce file date": "DIVORCE",
            "probate open date": "PROBATE",
            "personal representative": "PROBATE",
            "attorney on file": "PROBATE",
            "deed": "DEED_CHANGE",
            "last sold": "PROPERTY_SALE",
            "owned since": "OWNERSHIP_DURATION",
        }
        TAG_EVENT_PATTERNS = {
            r"skip traced (\w+) (\d{2}/\d{4})": "SKIP_TRACED",
            r"list purchased (\w+) (\d{2}/\d{4})": "LIST_PURCHASED",
            r"readymode (\d{2}/\d{4})": "READYMODE_UPDATE",
            r"original owner": "ORIGINAL_OWNER",
            r"vacant": "VACANT_HOME",
            r"poor/fair condition": "POOR_CONDITION",
            r"probate": "PROBATE",
            r"quit claim": "QUIT_CLAIM_DEED"
        }
        life_events = []
        sale_reasons = []  # Initialize sale_reasons
        tags = owner_doc.get("tags", [])
        for field, event_type in known_life_event_fields.items():
            raw_value = mapped_row.get(field, "").strip()
            if not raw_value:
                continue
            event = {
                "apn": apn,
                "event_type": event_type,
                "source": "CSV Field",
                "source_detail": field,
                "event_date": None,
                "notification_date": datetime.now(timezone.utc),
                "last_updated": datetime.now(timezone.utc)
            }
            if any(kw in field.lower() for kw in ["date", "year", "since"]):
                try:
                    if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", raw_value):
                        event["event_date"] = datetime.strptime(raw_value, "%Y-%m-%d %H:%M:%S")
                    elif re.match(r"\d{2}/\d{4}", raw_value):
                        event["event_date"] = datetime.strptime(f"01/{raw_value}", "%d/%m/%Y")
                    else:
                        event["event_date"] = parse(raw_value)
                except Exception as e:
                    logger.warning(f"Failed to parse date for {field}: {str(e)}")
            life_events.append(event)
        for tag in tags:
            tag_lower = tag.lower()
            for pattern, event_type in TAG_EVENT_PATTERNS.items():
                if re.search(pattern, tag_lower):
                    event = {
                        "apn": apn,
                        "event_type": event_type,
                        "source": "Tag",
                        "source_detail": tag,
                        "notification_date": datetime.now(timezone.utc),
                        "last_updated": datetime.now(timezone.utc)
                    }
                    date_match = re.search(r"(\d{2}/\d{4})", tag)
                    if date_match:
                        try:
                            event["event_date"] = datetime.strptime(
                                f"01/{date_match.group(1)}", "%d/%m/%Y"
                            )
                        except Exception as e:
                            logger.warning(f"Failed to parse date from tag: {tag} - {str(e)}")
                    life_events.append(event)
                    break
            if "tired landlords" in tag_lower:
                sale_reasons.append("TIRED_LANDLORD")
            if "empty nesters" in tag_lower:
                sale_reasons.append("EMPTY_NESTERS")
            if "high equity" in tag_lower:
                sale_reasons.append("HIGH_EQUITY")
            if any(indicator in tag_lower for indicator in ["poor condition", "fair condition"]):
                life_events.append({
                    "apn": apn,
                    "event_type": "PHYSICAL_DISTRESS",
                    "source": "Tag Analysis",
                    "notification_date": datetime.now(timezone.utc),
                    "last_updated": datetime.now(timezone.utc)
                })
        if sale_reasons:
            life_events.append({
                "apn": apn,
                "event_type": "SALE_REASON",
                "source": "Tag Analysis",
                "details": sale_reasons,
                "notification_date": datetime.now(timezone.utc),
                "last_updated": datetime.now(timezone.utc)
            })
        last_sold = mapped_row.get("last sold", "").strip()
        sale_price = mapped_row.get("last sale price", "").strip()
        if last_sold and sale_price and last_sold.lower() not in ["none", "n/a", ""]:
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
    prop_ops = []
    owner_ops = []
    phone_ops = []
    life_event_ops = []
    errors = []
    for row in batch:
        try:
            row_number = row.get("row_number", -1)
            raw_data = row.get("data", {})
            entities = process_unified_row(raw_data, db)
            if not entities:
                continue
            if "error" in entities:
                entities["error"]["row_number"] = row_number
                errors.append(entities["error"])
                logger.warning(f"[Row {row_number}] Skipped: {entities['error']['message']}")
                continue
            prop_ops.append(UpdateOne(
                {"apn": entities["property"]["apn"]},
                {"$set": entities["property"]},
                upsert=True
            ))
            owner_update = {
                "$setOnInsert": {
                    "normalized_owner_id": entities["owner"]["normalized_owner_id"],
                    "apn": entities["owner"]["apn"],
                    "full_name": entities["owner"]["full_name"],
                    "mailing_address": entities["owner"]["mailing_address"]
                },
                "$addToSet": {
                    "emails": {"$each": entities["owner"]["emails"]},
                    "tags": {"$each": entities["owner"]["tags"]}
                },
                "$set": {
                    "status": entities["owner"]["status"],
                    "last_updated": entities["owner"]["last_updated"]
                }
            }
            if entities["owner"]["phone_ids"]:
                owner_update["$addToSet"]["phone_ids"] = {"$each": entities["owner"]["phone_ids"]}
            owner_ops.append(UpdateOne(
                {"normalized_owner_id": entities["owner"]["normalized_owner_id"]},
                owner_update,
                upsert=True
            ))
            for phone in entities["phones"]:
                if not all(key in phone for key in ["phone_id", "number"]):
                    logger.error(f"Invalid phone document: {phone}")
                    continue
                logger.info(f"[Mongo Upsert] Inserting phone: {phone['number']} (ID: {phone['phone_id']})")
                phone_ops.append(UpdateOne(
                    {"number": str(phone["number"])},
                    {
                        "$setOnInsert": {
                            "phone_id": phone["phone_id"],
                            "number": str(phone["number"]),
                            "type": phone["type"],
                            "status": phone["status"],
                            "tags": phone.get("tags", []),
                            "created_at": phone["last_updated"]
                        },
                        "$set": {
                            "last_updated": phone["last_updated"]
                        },
                        "$addToSet": {
                            "linked_apns": {"$each": phone["linked_apns"]},
                            "linked_owners": {"$each": phone["linked_owners"]}
                        }
                    },
                    upsert=True
                ))
            for event in entities["life_events"]:
                life_event_ops.append(UpdateOne(
                    {
                        "apn": event["apn"],
                        "event_type": event["event_type"],
                        "source_detail": event["source_detail"]
                    },
                    {
                        "$setOnInsert": {
                            "created_at": datetime.now(timezone.utc)
                        },
                        "$set": {
                            "event_date": event.get("event_date"),
                            "notification_date": event["notification_date"],
                            "last_updated": event["last_updated"],
                            "source": event["source"]
                        },
                        "$addToSet": {
                            "related_tags": {"$each": [event["source_detail"]]}
                        }
                    },
                    upsert=True
                ))
        except Exception as e:
            logger.error(f"Batch processing error: {str(e)}")
    results = {}
    try:
        if prop_ops:
            results["properties"] = db.properties.bulk_write(prop_ops).bulk_api_result
        if owner_ops:
            results["owners"] = db.owners.bulk_write(owner_ops).bulk_api_result
        if phone_ops:
            try:
                bulk_result = db.phones.bulk_write(phone_ops)
                logger.info(
                    f"PHONE BULK WRITE RESULT:\n"
                    f"Inserted: {bulk_result.inserted_count}\n"
                    f"Updated: {bulk_result.modified_count}\n"
                    f"Upserts: {len(bulk_result.upserted_ids)}"
                )
                results["phones"] = bulk_result.bulk_api_result
            except BulkWriteError as bwe:
                logger.error(f"PHONE WRITE FAILURE: {str(bwe)}")
        if life_event_ops:
            try:
                result = db.life_events.bulk_write(life_event_ops)
                logger.info(f"LifeEvents: Inserted={result.inserted_count}, Updated={result.modified_count}")
            except BulkWriteError as bwe:
                logger.error(f"LifeEvents Write Error: {json.dumps(bwe.details, indent=2)}")
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

def process_unified_upload(file_bytes: bytes, session_id: str, db):
    batch_size = 1000
    processed = 0
    errors = []
    try:
        csv_file = io.TextIOWrapper(io.BytesIO(file_bytes), encoding="utf-8")
        reader = csv.DictReader(csv_file)
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
        batch = []
        for row_num, row in enumerate(reader, 1):
            try:
                normalized_row = {
                    "row_number": row_num,
                    "data": {
                        normalize_column_name(k): str(v).strip() 
                        for k, v in row.items()
                    }
                }
                batch.append(normalized_row)
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
        if batch:
            process_unified_batch(batch, db)
            processed += len(batch)
        db.upload_sessions.update_one(
            {"upload_id": session_id},
            {"$set": {
                "status": "completed",
                "end_time": datetime.now(timezone.utc),
                "processed_count": processed,
                "error_count": len(errors),
                "errors": errors[:1000]
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

@app.get("/properties/{apn}", tags=["Query"])
def get_property(apn: str, db=Depends(get_db)):
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
    session = db.upload_sessions.find_one(
        {"upload_id": session_id},
        {"_id": 0}
    )
    if not session:
        raise HTTPException(404, detail="Session not found")
    if 'timestamp' in session:
        session['timestamp'] = session['timestamp'].isoformat()
    return session

@app.get("/upload/requirements/unified", tags=["System"])
async def get_unified_requirements():
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
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=report.keys())
        writer.writeheader()
        writer.writerow(report)
        output.seek(0)
        return StreamingResponse(output, media_type="text/csv")
    return report

@app.get("/upload/sessions/{session_id}/error_rows.csv", tags=["Reporting"])
def download_error_rows_csv(session_id: str, db=Depends(get_db)):
    session = db.upload_sessions.find_one({"upload_id": session_id})
    if not session or "errors" not in session:
        raise HTTPException(404, "No errors found")
    all_errors = session["errors"][:1000]
    fieldnames = set()
    for error in all_errors:
        fieldnames.update(error["raw_data"].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=sorted(fieldnames))
    writer.writeheader()
    for error in all_errors:
        writer.writerow({
            k: str(v) for k, v in error["raw_data"].items()
        })
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=errors_{session_id}.csv"}
    )

@app.post("/fallback/enrich_missing_apn", tags=["Enrichment"])
def enrich_missing_apns(limit: int = 10, skip_already_enriched: bool = True, db: Database = Depends(get_db)):
    logger.info("🟢 /fallback/enrich_missing_apn POST hit")

    # --- Configuration ---
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    CONFIDENCE_THRESHOLD = 80
    APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

    # --- Metrics ---
    metrics = {
        "processed": 0,
        "sources": {"local_db": 0, "google": 0, "apify": 0},
        "durations": [],
        "errors": [],
        "errors_breakdown": {"timeout": 0, "captcha": 0, "no_apn_found": 0}
    }

    # --- Query Candidates ---
    match_filter = {
        "reason": {"$in": ["missing_apn", "apn_not_numeric_or_too_short"]}
    }
    if skip_already_enriched:
        match_filter["enrichment_status"] = {"$in": [None, "failed"]}

    candidates = list(db.fallback_candidates.find(match_filter).limit(limit))
    results = []

    def retry_wrapper(func, *args, label="", **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"{label} attempt {attempt+1} failed: {str(e)}")
                if "timeout" in str(e).lower():
                    metrics["errors_breakdown"]["timeout"] += 1
                elif "captcha" in str(e).lower():
                    metrics["errors_breakdown"]["captcha"] += 1
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(RETRY_DELAY * (attempt + 1))

    for candidate in candidates:
        start_time = time.time()
        candidate_id = candidate["_id"]
        raw_data = candidate.get("raw_data", {})
        metrics["processed"] += 1
        status = "failed"
        apn = None
        method = None
        confidence = None

        try:
            raw_address = raw_data.get("property address", "").strip()
            city = raw_data.get("property city", "").strip()
            state = raw_data.get("property state", "").strip()
            zip_code = raw_data.get("property zip", "").strip()

            if not all([raw_address, city, state]):
                raise ValueError("Missing address fields")

            standardized = standardize_address(raw_address, city, state, zip_code)
            full_address = f"{standardized['street']}, {standardized['city']}, {standardized['state']} {standardized['zip']}"

            # --- Step 1: Local DB fuzzy match ---
            try:
                owner_name = f"{raw_data.get('first name', '')} {raw_data.get('last name', '')}".strip()
                match, confidence = retry_wrapper(
                    find_best_db_match,
                    standardized['street'], owner_name, db,
                    label="Local DB"
                )
                if confidence >= CONFIDENCE_THRESHOLD:
                    apn = match["apn"]
                    method = "local_db"
                    status = "enriched_via_local_db"
                    metrics["sources"]["local_db"] += 1
                    results.append(success_result(candidate_id, apn, confidence, method))
                    move_out_of_fallback(apn, raw_data, db, candidate_id)
                    continue
            except Exception as e:
                logger.info(f"No local DB match for {candidate_id}: {e}")

            # --- Step 2: Google Scrape ---
            search_term = f"{full_address} parcel number -site:zillow.com"
            try:
                apn = retry_wrapper(get_parcel_number, search_term, str(candidate_id), label="Google")
                if apn:
                    method = "google"
                    status = "enriched_via_google"
                    metrics["sources"]["google"] += 1
                    results.append(success_result(candidate_id, apn, None, method))
                    move_out_of_fallback(apn, raw_data, db, candidate_id)
                    continue
            except Exception as e:
                logger.warning(f"Google scraping failed for {candidate_id}: {e}")

            # --- Step 3: Apify Backup ---
            if APIFY_TOKEN:
                try:
                    apn = retry_wrapper(apify_general_scrape, full_address, APIFY_TOKEN, label="Apify")
                    if apn:
                        method = "apify"
                        status = "enriched_via_apify"
                        metrics["sources"]["apify"] += 1
                        results.append(success_result(candidate_id, apn, None, method))
                        move_out_of_fallback(apn, raw_data, db, candidate_id)
                        continue
                except Exception as e:
                    logger.warning(f"Apify scrape failed for {candidate_id}: {e}")

            # --- All steps failed ---
            logger.warning(f"No APN found for {candidate_id}")
            metrics["errors_breakdown"]["no_apn_found"] += 1
            results.append({
                "id": str(candidate_id),
                "status": "failed",
                "apn": None,
                "confidence": None
            })

        except Exception as e:
            error_msg = f"💥 Critical error for {candidate_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            metrics["errors"].append(error_msg)
            results.append({
                "id": str(candidate_id),
                "status": "error",
                "error": str(e),
                "apn": None,
                "confidence": None
            })

        finally:
            duration = time.time() - start_time
            metrics["durations"].append(duration)
            try:
                db.fallback_candidates.update_one(
                    {"_id": candidate_id},
                    {
                        "$set": {
                            "enrichment_attempted_at": datetime.now(timezone.utc),
                            "enrichment_status": status,
                            "enrichment_method": method,
                            "enrichment_apn": apn,
                            "enrichment_confidence": confidence,
                            "processing_time": duration
                        }
                    }
                )
            except Exception as e:
                logger.error(f"Failed to update candidate {candidate_id}: {str(e)}")
            time.sleep(random.uniform(1, 3))

        # Optional: intermediate logging for large batches
        if metrics["processed"] % 5 == 0:
            logger.info(f"Progress: {metrics['processed']} processed, "
                        f"{sum(metrics['sources'].values())} successful so far.")

    success_count = sum(metrics["sources"].values())
    avg_time = sum(metrics["durations"]) / len(metrics["durations"]) if metrics["durations"] else 0
    success_rate = (success_count / metrics["processed"] * 100) if metrics["processed"] else 0

    logger.info(f"""
    🔚 APN Enrichment Finished
    --------------------------
    Processed: {metrics["processed"]}
    Success Rate: {success_rate:.1f}%
    Source Breakdown: {metrics["sources"]}
    Avg Time: {avg_time:.2f}s
    Errors: {len(metrics["errors"])}
    Error Breakdown: {metrics["errors_breakdown"]}
    """)

    return {
        "processed_count": len(results),
        "results": results,
        "metrics": metrics,
        "summary": {
            "success_rate": round(success_rate, 1),
            "google_wins": metrics["sources"]["google"],
            "db_wins": metrics["sources"]["local_db"],
            "apify_wins": metrics["sources"]["apify"],
            "errors": len(metrics["errors"]),
            "average_time_sec": round(avg_time, 2)
        }
    }


@app.post("/scrape/kingcounty/json", tags=["Scraping"])
async def scrape_kingcounty_json(file: UploadFile = File(...)) -> List[dict]:
    try:
        # Save uploaded file to a temporary path
        temp_input = os.path.join(tempfile.gettempdir(), file.filename)
        with open(temp_input, "wb") as f:
            f.write(await file.read())

        # Output file path
        temp_output = os.path.join(tempfile.gettempdir(), f"scraped_{file.filename}")

        # Run the scraper
        scrape_king_county_properties(temp_input, temp_output)

        # Load and return results as JSON
        df = pd.read_csv(temp_output)
        return df.to_dict(orient="records")

    except Exception as e:
        logging.error(f"King County scraping failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Scraper failed: " + str(e))
    

@app.post("/scrape/kingcounty/mongo", tags=["Scraping"])
def scrape_kingcounty_from_mongo(
    limit: int = 10,
    db: Database = Depends(get_db)
):
    """
    Scrapes King County property data for existing properties in MongoDB using their APNs.
    Stores results into each document under `scraped_data`.
    """
    try:
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("DB_NAME", "RealEstate")
        collection_name = "properties"

        if not mongo_uri:
            raise HTTPException(status_code=500, detail="MONGO_URI not configured")

        scrape_from_mongo_and_update(
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection_name=collection_name,
            limit=limit
        )

        return {
            "status": "completed",
            "message": f"Scraping finished for up to {limit} properties."
        }

    except Exception as e:
        logger.error(f"KingCounty Mongo scraping failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Mongo-based scrape failed: " + str(e))

@app.post("/enrich/violations", tags=["Enrichment"])
def enrich_violations(limit: int = 50, db=Depends(get_db)):
    from scrapers.code_violation import enrich_seattle_violations
    return enrich_seattle_violations(limit=limit)


@app.get("/", tags=["System"])
def root():
    return {
        "message": "Welcome to the Unified Real Estate Data API.",
        "version": "1.0.0",
        "endpoints": {
            "upload": "/upload/unified",
            "status": "/upload/sessions/{session_id}",
            "requirements": "/upload/requirements/unified",
            "property lookup": "/properties/{apn}"
        },
        "docs": "/docs"
    }

# --- Index Management ---

@app.on_event("startup")
async def startup_db_client():
    try:
        app.state.mongo_client = MongoClient(os.getenv("MONGO_URI"))
        app.state.db = app.state.mongo_client[os.getenv("DB_NAME", "PivotalRealEstate")]
        db = app.state.db
        if "upload_sessions" not in db.list_collection_names():
            db.create_collection("upload_sessions")
        db.properties.create_index([("apn", 1)], unique=True, background=True)
        db.properties.create_index([("address.zip", 1)], background=True)
        db.owners.create_index([("apn", 1)], background=True)
        db.owners.create_index([("phone_ids", 1)], background=True)
        db.phones.create_index(
            [("number", pymongo.ASCENDING)],
            name="number_unique_ci",
            unique=True,
            collation={"locale": "en", "strength": 2},
            background=True
        )
        db.life_events.create_index([("apn", 1), ("event_type", 1)])
        db.life_events.create_index([("event_date", -1)])
        if "fallback_candidates" not in db.list_collection_names():
            db.create_collection("fallback_candidates")
            logger.info("Created 'fallback_candidates' collection.")
        db.fallback_candidates.create_index([("status", 1)], background=True)
        db.fallback_candidates.create_index([("reason", 1)], background=True)
        db.fallback_candidates.create_index([("created_at", -1)], background=True)
        logger.info("Database connection established and indexes verified")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_db_client():
    if hasattr(app.state, "mongo_client"):
        app.state.mongo_client.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)