from typing import Dict, Any, List
from .base import BaseMigrator, RelationshipHandler
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient
from utils.parse_util import safe_int_parse

class ModelMigrator(BaseMigrator):
    """Handles model migration."""
    def __init__(self, mongo_client: MongoDBClient, dgraph_client: DgraphClient):
        super().__init__(mongo_client, dgraph_client)
        self.relationship_handler = RelationshipHandler(dgraph_client)

    async def _prepare_model_data(self, basic_metadata: Dict[str, Any], card_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare model data for upsert."""
        downloads = basic_metadata.get("downloads", {})
        if not isinstance(downloads, dict):
            downloads = {}
        
        current_downloads = safe_int_parse(downloads.get("current"))
        all_time_downloads = safe_int_parse(downloads.get("all_time"))

        return {
            "name": basic_metadata.get("id", ""),
            "created_at": basic_metadata.get("created_at", ""),
            "last_modified": basic_metadata.get("last_modified", ""),
            "downloads": str(max(current_downloads, all_time_downloads)),
            "likes": str(safe_int_parse(basic_metadata.get("likes"))),
            "base_model_relation": card_data.get("base_model_relation", "")
        }

    async def migrate(self) -> None:
        """Migrate models from MongoDB to Dgraph."""
        self.logger.info("Starting model migration...")
        cursor = self.mongo_client.client["huggingface_scraper"]["models"].find({})
        
        async for doc in cursor:
            try:
                basic_metadata = await self._extract_basic_metadata(doc)
                extended_metadata = await self._extract_extended_metadata(doc)
                card_data = basic_metadata.get("card_data") or {}
                status = basic_metadata.get("status") or {}
                author = basic_metadata.get("author") or ""
                tags = basic_metadata.get("tags") or []

                if await self._should_skip(status):
                    continue
                    
                if safe_int_parse(basic_metadata.get("likes")) < 5:
                    continue

                # Prepare and upsert model data
                model_data = await self._prepare_model_data(basic_metadata, card_data)
                await self.dgraph_client.upsert_model(model_data)

                # Prepare relationships
                relationships = []

                # Handle author
                author_rel = await self.relationship_handler.handle_author_relationship(
                    author, model_data["name"], "Model"
                )
                if author_rel:
                    relationships.append(author_rel)

                # Handle likers
                liker_rels = await self.relationship_handler.handle_likers(
                    extended_metadata.get("likers", []),
                    model_data["name"],
                    "Model"
                )
                relationships.extend(liker_rels)

                # Handle contributors
                contributor_rels = await self.relationship_handler.handle_contributors(
                    extended_metadata.get("contributors", []),
                    model_data["name"],
                    "Model"
                )
                relationships.extend(contributor_rels)


                tag_rels = await self.relationship_handler.handle_tags(
                    model_data["name"],
                    "Model",
                    tags
                )
                relationships.extend(tag_rels)

                # Handle base models
                if card_data.get("base_model"):
                    base_model_rels = await self.relationship_handler.handle_base_models(
                        model_data["name"],
                        card_data["base_model"]
                    )
                    relationships.extend(base_model_rels)

                # Handle datasets
                if card_data.get("datasets"):
                    if isinstance(card_data["datasets"], str):
                        card_data["datasets"] = [card_data["datasets"]]
                    dataset_rels = await self.relationship_handler.handle_trained_on(
                        model_data["name"],
                        card_data["datasets"]
                    )
                    relationships.extend(dataset_rels)

                # Create all relationships
                await self.relationship_handler.create_relationships(relationships)

            except Exception as e:
                self.logger.error(f"Error migrating model {doc.get('_id')}: {str(e)}")
                continue 