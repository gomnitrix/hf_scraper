from typing import Dict, Any
from datetime import datetime
from .base import BaseMigrator, RelationshipHandler
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient
from utils.parse_util import safe_int_parse

class OrganizationMigrator(BaseMigrator):
    """Handles organization migration."""
    def __init__(self, mongo_client: MongoDBClient, dgraph_client: DgraphClient):
        super().__init__(mongo_client, dgraph_client)
        self.relationship_handler = RelationshipHandler(dgraph_client)

    async def _prepare_org_data(self, doc: Dict[str, Any], basic_metadata: Dict[str, Any], 
                              extended_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare organization data for upsert."""
        return {
            "name": doc["_id"],
            "followers": str(safe_int_parse(extended_metadata.get("followers_count"))),
            "last_modified": basic_metadata.get("last_updated", datetime.now().isoformat())
        }

    async def migrate(self) -> None:
        """Migrate organizations from MongoDB to Dgraph."""
        self.logger.info("Starting organization migration...")
        cursor = self.mongo_client.client["huggingface_scraper"]["organizations"].find({})
        
        async for doc in cursor:
            try:
                basic_metadata = await self._extract_basic_metadata(doc)
                extended_metadata = await self._extract_extended_metadata(doc)
                
                # Prepare and upsert organization data
                org_data = await self._prepare_org_data(doc, basic_metadata, extended_metadata)
                await self.dgraph_client.upsert_organization(org_data)
                
                # Prepare relationships
                relationships = []

                # Handle followers
                if extended_metadata.get("followers_list"):
                    follower_rels = await self.relationship_handler.handle_followers(
                        org_data["name"],
                        "Organization",
                        extended_metadata["followers_list"]
                    )
                    relationships.extend(follower_rels)

                # Handle members
                if basic_metadata.get("members"):
                    member_rels = await self.relationship_handler.handle_members(
                        org_data["name"],
                        basic_metadata["members"]
                    )
                    relationships.extend(member_rels)

                # Create all relationships
                await self.relationship_handler.create_relationships(relationships)

            except Exception as e:
                self.logger.error(f"Error migrating organization {doc.get('_id')}: {str(e)}")
                continue 