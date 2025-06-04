from pymongo import MongoClient
import os

client = None

async def connect_to_mongo(app):
    global client
    client = MongoClient(os.getenv("MONGO_URI"))
    app.state.db = client[os.getenv("DB_NAME", "RealEstate")]

async def close_mongo_connection(app):
    global client
    if client:
        client.close()

def get_db():
    global client
    if not client:
        raise RuntimeError("Database connection is not established")
    return client[os.getenv("DB_NAME", "RealEstate")]
