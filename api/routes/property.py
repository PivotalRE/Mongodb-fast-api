from fastapi import APIRouter, Depends, HTTPException
from bson import json_util
import json
from api.utils.helpers import clean_apn
from api.db.connection import get_db

router = APIRouter()

@router.get("/properties/{apn}", tags=["Query"])
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
