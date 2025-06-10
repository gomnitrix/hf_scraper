from typing import Dict, Any, List, Tuple
import logging
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient

class BaseMigrator:
    """Base class for all migrators."""
    def __init__(self, mongo_client: MongoDBClient, dgraph_client: DgraphClient):
        self.mongo_client = mongo_client
        self.dgraph_client = dgraph_client
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _should_skip(self, status: Dict[str, Any]) -> bool:
        """Check if the item should be skipped based on its status."""
        return status.get("private", False) or status.get("disabled", False) or status.get("gated", False)

    async def _extract_basic_metadata(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic metadata from document."""
        return doc.get("basic_metadata", {})

    async def _extract_extended_metadata(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract extended metadata from document."""
        return doc.get("extended_metadata", {})

class RelationshipHandler:
    """Handles relationship creation between nodes."""
    def __init__(self, dgraph_client: DgraphClient):
        self.dgraph_client = dgraph_client
        self.logger = logging.getLogger(self.__class__.__name__)

    async def create_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """Create relationships between nodes."""
        if relationships:
            await self.dgraph_client.create_relationships(relationships)

    async def handle_author_relationship(self, author: str, target_name: str, target_type: str) -> Dict[str, Any]:
        """Handle author relationship creation."""
        if not author:
            return None
        
        organization = await self.dgraph_client._get_node_uid("Organization", author)
        if not organization:
            await self.dgraph_client.upsert_user({"name": author})
        
        return {
            "from_name": author,
            "from_type": "User" if not organization else "Organization",
            "to_name": target_name,
            "to_type": target_type,
            "predicate": "author_of"
        }

    async def handle_owner_relationship(self, owner: str, target_name: str, target_type: str) -> Dict[str, Any]:
        """Handle owner relationship creation."""
        if not owner:
            return None
        
        organization = await self.dgraph_client._get_node_uid("Organization", owner)
        if not organization:
            await self.dgraph_client.upsert_user({"name": owner})
        
        return {
            "from_name": owner,
            "from_type": "User" if not organization else "Organization",
            "to_name": target_name,
            "to_type": target_type,
            "predicate": "owner_of"
        }

    async def handle_likers(self, likers: List[str], target_name: str, target_type: str) -> List[Dict[str, Any]]:
        """Handle likers relationship creation."""
        relationships = []
        if likers:
            for liker in likers:
                await self.dgraph_client.upsert_user({"name": liker})
                relationships.append({
                    "from_name": liker,
                    "from_type": "User",
                    "to_name": target_name,
                    "to_type": target_type,
                    "predicate": "like"
                })
        return relationships
    
    async def handle_contributors(self, contributors: List[str], target_name: str, target_type: str) -> List[Dict[str, Any]]:
        """Handle contributor relationships."""
        relationships = []
        for contributor in contributors:
            await self.dgraph_client.upsert_user({"name": contributor})
            relationships.append({
                "from_name": contributor,
                "from_type": "User",
                "to_name": target_name,
                "to_type": target_type,
                "predicate": "contribute_to"
            })
        return relationships

    async def handle_base_models(self, model_name: str, base_models: Any) -> List[Dict[str, Any]]:
        """Handle base model relationships."""
        relationships = []
        if isinstance(base_models, str):
            base_models = [base_models]
        
        for base_model in base_models:
            await self.dgraph_client.upsert_model({"name": base_model})
            relationships.append({
                "from_name": model_name,
                "from_type": "Model",
                "to_name": base_model,
                "to_type": "Model",
                "predicate": "based_on"
            })
        return relationships

    async def handle_trained_on(self, model_name: str, datasets: List[str]) -> List[Dict[str, Any]]:
        """Handle dataset relationships."""
        relationships = []
        for dataset in datasets:
            await self.dgraph_client.upsert_dataset({"name": dataset})
            relationships.append({
                "from_name": model_name,
                "from_type": "Model",
                "to_name": dataset,
                "to_type": "Dataset",
                "predicate": "trained_on"
            })
        return relationships

    async def handle_tags(self, source_name: str, source_type: str, tags: List[str]) -> List[Dict[str, Any]]:
        """Handle tag relationships."""
        relationships = []
        for tag in tags:
            await self.dgraph_client.upsert_tag({"name": tag})
            relationships.append({
                "from_name": source_name,
                "from_type": source_type,
                "to_name": tag,
                "to_type": "Tag",
                "predicate": "has_tag"
            })
        return relationships

    async def handle_libraries(self, source_name: str, source_type: str, libraries: List[str]) -> List[Dict[str, Any]]:
        """Handle library relationships."""
        relationships = []
        for library in libraries:
            await self.dgraph_client.upsert_library({"name": library})
            relationships.append({
                "from_name": source_name,
                "from_type": source_type,
                "to_name": library,
                "to_type": "Library",
                "predicate": "in_library"
            })
        return relationships

    async def handle_licenses(self, source_name: str, source_type: str, licenses: List[str]) -> List[Dict[str, Any]]:
        """Handle license relationships."""
        relationships = []
        for license in licenses:
            await self.dgraph_client.upsert_license({"name": license})
            relationships.append({
                "from_name": source_name,
                "from_type": source_type,
                "to_name": license,
                "to_type": "License",
                "predicate": "has_license"
            })
        return relationships

    async def handle_collection_items(self, collection_name: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Handle collection item relationships."""
        relationships = []
        for item in items:
            item_id = item.get("item_id")
            item_type = item.get("item_type")
            if item_type not in ["model", "dataset"]:
                continue
            
            # filter out models that are not in the database (or we can upsert them)
            if item_type == "model":
                # await self.dgraph_client.upsert_model({"name": item_id})
                item_uid = await self.dgraph_client._get_node_uid("Model", item_id)
            else:
                # await self.dgraph_client.upsert_dataset({"name": item_id})
                item_uid = await self.dgraph_client._get_node_uid("Dataset", item_id)
            if not item_uid:
                continue
            
            relationships.append({
                "from_name": collection_name,
                "from_type": "Collection",
                "to_name": item_id,
                "to_type": "Model" if item_type == "model" else "Dataset",
                "predicate": "contains"
            })
        return relationships

    async def handle_followers(self, target_name: str, target_type: str, followers: List[str]) -> List[Dict[str, Any]]:
        """Handle follower relationships."""
        relationships = []
        for follower in followers:
            await self.dgraph_client.upsert_user({"name": follower})
            relationships.append({
                "from_name": follower,
                "from_type": "User",
                "to_name": target_name,
                "to_type": target_type,
                "predicate": "follow"
            })
        return relationships

    async def handle_members(self, org_name: str, members: List[str]) -> List[Dict[str, Any]]:
        """Handle organization member relationships."""
        relationships = []
        for member in members:
            await self.dgraph_client.upsert_user({"name": member})
            relationships.append({
                "from_name": member,
                "from_type": "User",
                "to_name": org_name,
                "to_type": "Organization",
                "predicate": "member_of"
            })
        return relationships

    async def handle_upvoters(self, target_name: str, target_type: str, upvoters: List[str]) -> List[Dict[str, Any]]:
        """Handle upvoter relationships."""
        relationships = []
        for upvoter in upvoters:
            await self.dgraph_client.upsert_user({"name": upvoter})
            relationships.append({
                "from_name": upvoter,
                "from_type": "User",
                "to_name": target_name,
                "to_type": target_type,
                "predicate": "upvote"
            })
        return relationships 