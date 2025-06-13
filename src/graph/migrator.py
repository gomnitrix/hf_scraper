import logging
import asyncio
from typing import List, Dict, Any
from utils.dgraph_client import DgraphClient
from utils.mongodb import MongoDBClient
from .model_migrator import ModelMigrator
from .dataset_migrator import DatasetMigrator
from .organization_migrator import OrganizationMigrator
from .collection_migrator import CollectionMigrator

class GraphMigrator:
    """Main migrator class that coordinates all migrations."""
    def __init__(self, mongo_uri: str, dgraph_uri: str):
        self.mongo_client = MongoDBClient(mongo_uri)
        self.dgraph_client = DgraphClient(dgraph_uri)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize specialized migrators
        self.model_migrator = ModelMigrator(self.mongo_client, self.dgraph_client)
        self.dataset_migrator = DatasetMigrator(self.mongo_client, self.dgraph_client)
        self.organization_migrator = OrganizationMigrator(self.mongo_client, self.dgraph_client)
        self.collection_migrator = CollectionMigrator(self.mongo_client, self.dgraph_client)

    async def _get_filtered_users(self, page_size: int = 3000) -> List[Dict[str, Any]]:
        """Get users that have only one relation with pagination."""
        query = f"""
        {{
            var(func: type(User)) {{
                numLikes as count(like)
                numUpvotes as count(upvote)
                numFollow as count(follow)
                numMemberOf as count(member_of)
                numAuthorOf as count(author_of)
                numOwnerOf as count(owner_of)
                numContributeTo as count(contribute_to)

                totalInteraction as math(numLikes + numUpvotes + numFollow + numMemberOf)
                totalRelation as math(numAuthorOf + numOwnerOf + numContributeTo)

                allUsers as uid
            }}

            filteredUsers(func: uid(allUsers), first: {page_size}) @filter(lt(val(totalInteraction), 10) AND eq(val(totalRelation), 0)) {{
                uid
                name
                totalInteraction: val(totalInteraction)
                totalRelation: val(totalRelation)
            }}
        }}
        """
        try:
            result = await self.dgraph_client.query(query)
            return result.get("filteredUsers", [])
        except Exception as e:
            self.logger.error(f"Failed to get filtered users: {str(e)}")
            raise

    async def delete_single_relation_users(self) -> None:
        """Delete User nodes that have only one relation using pagination."""
        total_deleted = 0

        while True:
            users = await self._get_filtered_users()
            if not users:
                break

            uids_to_delete = [user["uid"] for user in users]
            await self.dgraph_client.delete_nodes(uids_to_delete)
            total_deleted += len(uids_to_delete)

        if total_deleted > 0:
            self.logger.info(f"Successfully deleted {total_deleted} users with single relation")
        else:
            self.logger.info("No users found with single relation")

    async def migrate_all(self) -> None:
        """Migrate all data from MongoDB to Dgraph."""
        try:
            # Read and set schema
            with open("src/graph/schema.rdf", "r") as f:
                schema = f.read()
            await self.dgraph_client.alter_schema(schema)
            
            # Migrate data
            await self.organization_migrator.migrate()

            await asyncio.gather(
                self.model_migrator.migrate(),
                self.dataset_migrator.migrate(),
                self.collection_migrator.migrate()
            )

            # Clean up single relation users
            await self.delete_single_relation_users()
            
            self.logger.info("Migration completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during migration: {str(e)}")
            raise 