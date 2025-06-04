from datetime import datetime, timezone
import logging
from typing import List, Dict, Optional
from fastapi.security import APIKeyHeader
import json

from pymongo.errors import BulkWriteError
from email_validator import validate_email, EmailNotValidError
import hashlib
import re
from dateutil.parser import parse

logger = logging.getLogger(__name__)

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

def get_mongo_operators():
    from pymongo import UpdateOne
    return UpdateOne


import re
from typing import Any, Dict, List, Optional

def safe_int(value: Any) -> Optional[int]:
    try: return int(float(value)) if value not in [None, ""] else None
    except: return None

def safe_float(value: Any) -> Optional[float]:
    try: return float(value) if value not in [None, ""] else None
    except: return None

def clean_apn(raw_apn: str) -> Optional[str]:
    cleaned = re.sub(r"[^\d]", "", str(raw_apn).strip())
    if not cleaned.isdigit():
        return None
    return cleaned.zfill(10)

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


# Import or define process_unified_row before using it
def process_unified_batch(batch: List[Dict], db):
    UpdateOne = get_mongo_operators()  # lazy import to avoid circular issues

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
                results["phones"] = db.phones.bulk_write(phone_ops).bulk_api_result
            except BulkWriteError as bwe:
                logger.error(f"PHONE WRITE FAILURE: {str(bwe)}")
        if life_event_ops:
            try:
                results["life_events"] = db.life_events.bulk_write(life_event_ops).bulk_api_result
            except BulkWriteError as bwe:
                logger.error(f"LifeEvents Write Error: {json.dumps(bwe.details, indent=2)}")
    except BulkWriteError as bwe:
        logger.error(f"Bulk write error: {bwe.details}")
        results["errors"] = bwe.details

    return results
