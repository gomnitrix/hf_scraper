from typing import Dict, Any, List, Tuple
from .base import BaseMigrator, RelationshipHandler
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient

class DatasetMigrator(BaseMigrator):
    """Handles dataset migration."""
    def __init__(self, mongo_client: MongoDBClient, dgraph_client: DgraphClient):
        super().__init__(mongo_client, dgraph_client)
        self.relationship_handler = RelationshipHandler(dgraph_client)

    async def _prepare_dataset_data(self, basic_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare dataset data for upsert."""
        downloads = basic_metadata.get("downloads", {})
        if not isinstance(downloads, dict):
            downloads = {}
        
        current_downloads = downloads.get("current", 0)
        all_time_downloads = downloads.get("all_time", 0)
        
        if not isinstance(current_downloads, (int, float)):
            current_downloads = 0
        if not isinstance(all_time_downloads, (int, float)):
            all_time_downloads = 0
        return {
            "name": basic_metadata.get("id", ""),
            "created_at": basic_metadata.get("created_at", ""),
            "last_modified": basic_metadata.get("last_modified", ""),
            "downloads": str(max(current_downloads, all_time_downloads)),
            "likes": str(basic_metadata.get("likes", 0))
        }

    async def _process_tags(self, tags: List[str]) -> Tuple[List[str], List[str], List[str], List[str]]:
        """Process tags into different categories."""
        filtered_tags = []
        libraries = []
        licenses = []
        arxiv_ids = []

        for tag in tags:
            if tag.startswith("library:"):
                libraries.append(tag.split(":", 1)[1])
            elif tag.startswith("license:"):
                licenses.append(tag.split(":", 1)[1])
            elif tag.startswith("arxiv:"):
                arxiv_ids.append(tag.split(":", 1)[1])
            else:
                filtered_tags.append(tag)

        return filtered_tags, libraries, licenses, arxiv_ids

    async def migrate(self) -> None:
        """Migrate datasets from MongoDB to Dgraph."""
        self.logger.info("Starting dataset migration...")
        cursor = self.mongo_client.client["huggingface_scraper"]["datasets"].find({})
        
        async for doc in cursor:
            try:
                basic_metadata = await self._extract_basic_metadata(doc)
                extended_metadata = await self._extract_extended_metadata(doc)
                status = basic_metadata.get("status", {})
                author = basic_metadata.get("author", "")

                if await self._should_skip(status):
                    continue

                # Process tags
                filtered_tags, libraries, licenses, arxiv_ids = await self._process_tags(
                    basic_metadata.get("tags", [])
                )

                # Prepare and upsert dataset data
                dataset_data = await self._prepare_dataset_data(basic_metadata)
                dataset_data["arxiv_ids"] = arxiv_ids
                await self.dgraph_client.upsert_dataset(dataset_data)

                # Prepare relationships
                relationships = []

                # Handle author
                author_rel = await self.relationship_handler.handle_author_relationship(
                    author, dataset_data["name"], "Dataset"
                )
                if author_rel:
                    relationships.append(author_rel)

                # Handle likers
                liker_rels = await self.relationship_handler.handle_likers(
                    extended_metadata.get("likers", []),
                    dataset_data["name"],
                    "Dataset"
                )
                relationships.extend(liker_rels)

                # Handle contributors
                contributor_rels = await self.relationship_handler.handle_contributors(
                    extended_metadata.get("contributors", []),
                    dataset_data["name"],
                    "Dataset"
                )
                relationships.extend(contributor_rels)

                # Handle tags
                tag_rels = await self.relationship_handler.handle_tags(
                    dataset_data["name"],
                    "Dataset",
                    filtered_tags
                )
                relationships.extend(tag_rels)

                # Handle libraries
                if basic_metadata.get("library_name"):
                    libraries.append(basic_metadata["library_name"])

                library_rels = await self.relationship_handler.handle_libraries(
                    dataset_data["name"],
                    "Dataset",
                    libraries
                )
                relationships.extend(library_rels)

                # Handle licenses
                license_rels = await self.relationship_handler.handle_licenses(
                    dataset_data["name"],
                    "Dataset",
                    licenses
                )
                relationships.extend(license_rels)

                # Create all relationships
                await self.relationship_handler.create_relationships(relationships)

            except Exception as e:
                self.logger.error(f"Error migrating dataset {doc.get('_id')}: {str(e)}")
                continue 