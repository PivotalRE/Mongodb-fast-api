from fastapi import FastAPI
from dotenv import load_dotenv
import logging

from api.routes import upload, property, fallback, system
# from db.connection import connect_to_mongo, close_mongo_connection
from api.db.connection import connect_to_mongo, close_mongo_connection

load_dotenv()
app = FastAPI(title="Unified Real Estate Data API", version="1.0.0")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

def get_db():
    return app.state.db

@app.on_event("startup")
async def startup():
    await connect_to_mongo(app)

@app.on_event("shutdown")
async def shutdown():
    await close_mongo_connection(app)

# Include route modules
app.include_router(upload.router)
app.include_router(property.router)
app.include_router(fallback.router)
app.include_router(system.router)

