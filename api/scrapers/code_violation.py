import requests
from pymongo import MongoClient
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
import logging
import re

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# DB setup
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("DB_NAME")]
properties = db["properties"]
violation_logs = db["violation_logs"]

API_URL = "https://data.seattle.gov/resource/ez4a-iug7.json"

# --------------------
# Address Normalization
# --------------------
def normalize_suffix(address):
    suffix_map = {
        "STREET": "ST",
        "AVENUE": "AVE",
        "PLACE": "PL",
        "ROAD": "RD",
        "BOULEVARD": "BLVD",
        "DRIVE": "DR",
        "COURT": "CT",
        "LANE": "LN",
        "TERRACE": "TER",
        "WAY": "WY"
    }
    for full, abbr in suffix_map.items():
        address = re.sub(rf"\b{full}\b", abbr, address)
    return address

def strip_unit_info(address):
    return re.sub(r"\s+(APT|UNIT|#)\s*\w+", "", address)

def clean_address(address: str) -> str:
    address = address.strip().upper()
    address = strip_unit_info(address)
    address = normalize_suffix(address)
    return address

# --------------------
# Violation Dataset Fetch
# --------------------
def fetch_violations_dataset():
    try:
        logger.info("Fetching Seattle violation dataset...")
        response = requests.get(API_URL)
        response.raise_for_status()
        logger.info("Violation dataset fetched successfully.")
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch violation dataset: {e}")
        return []

# --------------------
# Main Enrichment Function
# --------------------
def enrich_seattle_violations(limit=50, similarity_threshold=85):
    logger.info(f"🚀 Starting violation enrichment for up to {limit} Seattle properties...")

    enriched = 0
    skipped = 0
    failures = 0
    all_violations = fetch_violations_dataset()

    for prop in properties.find({
        "address.city": {"$regex": "^Seattle$", "$options": "i"},
        "has_violation": {"$exists": False}
    }).limit(limit):

        address = prop.get("address", {}).get("street", "")
        if not address:
            skipped += 1
            logger.warning(f"Skipping property with missing address: {prop.get('_id')}")
            continue

        address_query = clean_address(address)

        try:
            match_data = []
            match_type = "none"
            match_score = None

            # --- Preprocess all violations
            candidates = [
                (clean_address(v.get("originaladdress1", "")), v)
                for v in all_violations if v.get("originaladdress1")
            ]

            # --- Try exact match
            exact_matches = [v for addr, v in candidates if addr == address_query]
            if exact_matches:
                match_data = exact_matches
                match_type = "exact"
                logger.info(f"✅ Exact match found for: {address_query}")
            else:
                # --- Fuzzy match
                candidate_strings = [c[0] for c in candidates]
                extracted = process.extractOne(address_query, candidate_strings, scorer=fuzz.ratio)
                if extracted:
                    best_match, score, _ = extracted
                    match_score = score
                    if score >= similarity_threshold:
                        match_data = [v for addr, v in candidates if addr == best_match]
                        match_type = "fuzzy"
                        logger.info(f"🟡 Fuzzy match ({score}%) → {best_match} for {address_query}")
                    else:
                        logger.info(f"❌ No match above threshold for {address_query} (best: {best_match} @ {score}%)")
                else:
                    logger.info(f"❌ No candidates found for fuzzy matching {address_query}")

            # --- Update property
            if match_data:
                description = "; ".join(
                    d.get("description", "") for d in match_data if d.get("description")
                )
                properties.update_one(
                    {"_id": prop["_id"]},
                    {"$set": {
                        "violation_description": description,
                        "has_violation": True,
                        "violation_enriched_at": datetime.utcnow()
                    }}
                )
                enriched += 1
            else:
                properties.update_one(
                    {"_id": prop["_id"]},
                    {"$set": {
                        "has_violation": False,
                        "violation_enriched_at": datetime.utcnow()
                    }}
                )
                skipped += 1

            # --- Log result
            violation_logs.insert_one({
                "property_id": prop["_id"],
                "address": address_query,
                "timestamp": datetime.utcnow(),
                "has_violation": bool(match_data),
                "match_type": match_type,
                "fuzzy_score": match_score
            })

            time.sleep(0.2)

        except Exception as e:
            logger.error(f"💥 Error processing {address_query}: {e}")
            failures += 1

    logger.info(f"""
    🧾 Enrichment Summary:
    ------------------------
    Enriched: {enriched}
    Skipped: {skipped}
    Failures: {failures}
    Limit: {limit}
    """)

    return {
        "status": "completed",
        "enriched": enriched,
        "skipped": skipped,
        "failures": failures,
        "message": f"Finished violation enrichment for up to {limit} properties."
    }
