import re
import math
import hashlib
import urllib.parse
from typing import Dict, List, Any, Optional
from rapidfuzz import fuzz # type: ignore
import requests
import phonenumbers     # type: ignore
from api.utils.common import process_unified_row, process_unified_batch
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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