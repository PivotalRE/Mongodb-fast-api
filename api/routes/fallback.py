from fastapi import APIRouter, Depends
from pymongo.database import Database
from api.utils.enrichment import enrich_missing_apns
from api.db.connection import get_db 
router = APIRouter()

@router.post("/fallback/enrich_missing_apn", tags=["Enrichment"])
def enrich_apns(limit: int = 10, db: Database = Depends(get_db)):
    return enrich_missing_apns(limit=limit, db=db)
