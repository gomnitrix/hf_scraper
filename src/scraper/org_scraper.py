from typing import Dict, Any, Iterable, List
from .base_scraper import BaseScraper
from datetime import datetime
import logging
import aiohttp

class OrganizationScraper(BaseScraper):
    def __init__(self, 
                 mongo_uri: str,
                 redis_uri: str,
                 rate_limit: int = 10,
                 batch_size: int = 64):
        super().__init__(
            mongo_uri=mongo_uri,
            redis_uri=redis_uri,
            rate_limit=rate_limit,
            batch_size=batch_size
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def item_type(self) -> str:
        return "organizations"

    def get_item_id(self, item: Dict[str, Any]) -> str:
        return item["organization"]

    def get_item_metadata(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item["organization"],
            "members": item["members"],
            "last_updated": item["last_updated"]
        }

    async def list_items(self) -> Iterable[Dict[str, Any]]:
        """List all organizations from models and datasets."""
        self.logger.info("Listing organizations from models and datasets") 

        authors = set()
        for collection in ["models", "datasets"]:
            cursor = self.mongo_client.client[f"huggingface_scraper"][collection].find(
                {}, {"basic_metadata.author": 1, "_id": 0}
            )
            async for doc in cursor:
                if doc.get("basic_metadata", {}).get("author"):
                    authors.add(doc["basic_metadata"]["author"])
        
        self.logger.info(f"Found {len(authors)} unique authors to check")
        
        for author in authors:
            try:
                existing = await self.mongo_client.client["huggingface_scraper"]["organizations"].find_one(
                    {"organization": author}
                )
                if existing:
                    continue

                members = self.api.list_organization_members(author)
                if members is not None:
                    yield {
                        "organization": author,
                        "members": [member.username for member in members],
                        "last_updated": datetime.now().isoformat()
                    }
            except Exception as e:
                self.logger.error(f"Error checking organization {author}: {str(e)}")
                continue

    async def fetch_extended_metadata(self, item_id: str) -> Dict[str, Any]:
        """Fetch extended metadata (followers) for an item."""
        
        try:
            followers = await self._fetch_followers(item_id)
            
            return {
                'followers': followers,
                'followers_count': len(followers),
                'last_updated': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error fetching extended metadata for {item_id}: {str(e)}")
            raise
    
    async def _fetch_followers(self, org_id: str) -> List[str]:
        """Fetch followers for an item using HTTP API and return a list of usernames."""
        await self.redis_client.wait_for_rate_limit("followers", self.rate_limit)
        
        try:
            url = f"https://huggingface.co/api/organizations/{org_id}/followers"
            async with self.session.get(url) as response:
                if response.status == 200:
                    followers = await response.json()
                    return [follower['user'] for follower in followers]
                return []
        except Exception as e:
            self.logger.error(f"Error fetching followers for {org_id}: {str(e)}")
            return []
        