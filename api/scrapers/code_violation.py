import requests
from pymongo import MongoClient
from datetime import datetime
import time
import os
import sys
from dotenv import load_dotenv
load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("DB_NAME")]
properties = db["properties"]
violation_logs = db["violation_logs"]

API_URL = "https://data.seattle.gov/resource/ez4a-iug7.json"

def enrich_seattle_violations(limit=50):
    enriched = 0
    skipped = 0
    failures = 0

    for prop in properties.find({
        "address.city": {"$regex": "^Seattle$", "$options": "i"},
        "has_violation": {"$exists": False}
    }).limit(limit):

        address = prop.get("address", {}).get("street", "")
        if not address:
            skipped += 1
            continue

        address_query = address.upper()
        try:
            response = requests.get(API_URL, params={"originaladdress1": address_query})
            data = response.json()
            if data:
                description = "; ".join(d.get("description", "") for d in data if d.get("description"))
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

            violation_logs.insert_one({
                "property_id": prop["_id"],
                "address": address_query,
                "timestamp": datetime.utcnow(),
                "has_violation": bool(data)
            })

            time.sleep(0.2)

        except Exception as e:
            print(f"Error for {address}: {e}")
            failures += 1

    return {
        "status": "completed",
        "enriched": enriched,
        "skipped": skipped,
        "failures": failures,
        "message": f"Finished violation enrichment for up to {limit} properties."
    }
