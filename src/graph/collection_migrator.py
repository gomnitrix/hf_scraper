from typing import Dict, Any, List
from datetime import datetime
from .base import BaseMigrator, RelationshipHandler
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient
from utils.parse_util import safe_int_parse

class CollectionMigrator(BaseMigrator):
    """Handles collection migration."""
    def __init__(self, mongo_client: MongoDBClient, dgraph_client: DgraphClient):
        super().__init__(mongo_client, dgraph_client)
        self.relationship_handler = RelationshipHandler(dgraph_client)

    async def _prepare_collection_data(self, basic_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare collection data for upsert."""
        return {
            "id": basic_metadata.get("id", ""),
            "name": basic_metadata.get("title", ""),
            # "description": escape_special_chars(basic_metadata.get("description", "")),
            "last_modified": basic_metadata.get("last_updated", datetime.now().isoformat()),
            "upvotes": str(safe_int_parse(basic_metadata.get("upvotes")))
        }

    async def migrate(self) -> None:
        """Migrate collections from MongoDB to Dgraph."""
        self.logger.info("Starting collection migration...")
        cursor = self.mongo_client.client["huggingface_scraper"]["collections"].find({})
        
        async for doc in cursor:
            try:
                basic_metadata = await self._extract_basic_metadata(doc)
                extended_metadata = await self._extract_extended_metadata(doc)
                owner = basic_metadata.get("owner") or ""

                # Prepare and upsert collection data
                collection_data = await self._prepare_collection_data(basic_metadata)
                await self.dgraph_client.upsert_collection(collection_data)
                
                # Prepare relationships
                relationships = []
                
                # Handle owner
                if owner:
                    owner_rel = await self.relationship_handler.handle_owner_relationship(
                        owner, collection_data["name"], "Collection"
                    )
                    if owner_rel:
                        relationships.append(owner_rel)
                
                # Handle collection items
                if extended_metadata.get("items"):
                    item_rels = await self.relationship_handler.handle_collection_items(
                        collection_data["name"],
                        extended_metadata["items"]
                    )
                    relationships.extend(item_rels)
                
                # Handle upvoters
                if extended_metadata.get("upvoters"):
                    upvoter_rels = await self.relationship_handler.handle_upvoters(
                        collection_data["name"],
                        "Collection",
                        extended_metadata["upvoters"]
                    )
                    relationships.extend(upvoter_rels)
                
                # Create all relationships
                await self.relationship_handler.create_relationships(relationships)
                
            except Exception as e:
                self.logger.error(f"Error migrating collection {doc.get('_id')}: {str(e)}")
                continue 