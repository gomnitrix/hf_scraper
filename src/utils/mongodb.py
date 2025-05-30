from pymongo import UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, List
from datetime import datetime

class MongoDBClient:
    def __init__(self, mongo_uri: str):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client.huggingface_scraper

    async def upsert_basic_metadata(self, collection: str, item_id: str, metadata: Dict[str, Any]) -> None:
        """Upsert basic metadata for an item."""
        await self.db[collection].update_one(
            {"_id": item_id},
            {
                "$set": {
                    "basic_metadata": metadata,
                    "status": {
                        "phase": "basic",
                        "updated_at": datetime.now().isoformat()
                    }
                }
            },
            upsert=True
        )
    
    async def bulk_upsert_basic_metadata(self, collection: str, items: List[Dict[str, Any]]) -> None:
        """Bulk upsert basic metadata for a list of items."""
        operations = [
            UpdateOne(
                {"_id": item["id"]},
                {"$set": {
                    "basic_metadata": item,
                    "status": {
                        "phase": "basic",
                        "updated_at": datetime.now().isoformat()
                    }}},
                upsert=True
            )
            for item in items
        ]
        await self.bulk_write(collection, operations)
        
    async def bulk_write(self, collection: str, operations: List[Dict[str, Any]]) -> None:
        """Execute bulk write operations."""
        if operations:
            await self.db[collection].bulk_write(operations, ordered=False)

    async def upsert_extended_metadata(self, collection: str, item_id: str, metadata: Dict[str, Any]) -> None:
        """Upsert extended metadata for an item."""
        await self.db[collection].update_one(
            {"_id": item_id},
            {
                "$set": {
                    "extended_metadata": metadata,
                    "status.phase": "extended",
                    "status.updated_at": datetime.now().isoformat()
                }
            }
        )

    async def get_stats(self, collection: str) -> Dict[str, int]:
        """Get scraping statistics for a collection."""
        total = await self.db[collection].count_documents({})
        basic = await self.db[collection].count_documents({"status.phase": "basic"})
        extended = await self.db[collection].count_documents({"status.phase": "extended"})
        
        return {
            "total": total,
            "basic": basic,
            "extended": extended
        }

