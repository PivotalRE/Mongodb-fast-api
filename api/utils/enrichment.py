import logging
from typing import Optional
from rapidfuzz import fuzz # type: ignore
import requests
import urllib.parse
import re

logger = logging.getLogger(__name__)

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

def enrich_missing_apns(limit: int, db):
    # logic copied from fallback enrichment in your original code
    ...
