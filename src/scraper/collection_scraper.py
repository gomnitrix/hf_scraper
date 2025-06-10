from typing import Dict, Any, Iterable, List, Callable
from .base_scraper import BaseScraper
from huggingface_hub import Collection, get_collection
from datetime import datetime
import logging
import aiohttp

class CollectionFilter:
    """Collection filter class for flexible filtering of collections."""
    
    def __init__(self):
        self.filters: List[Callable[[Collection], bool]] = []
    
    def add_filter(self, filter_func: Callable[[Collection], bool]) -> 'CollectionFilter':
        """Add a filter function."""
        self.filters.append(filter_func)
        return self
    
    def filter_by_title(self, title: str) -> 'CollectionFilter':
        """Filter collections by title (case-insensitive contains)."""
        return self.add_filter(lambda c: title.lower() in c.title.lower())
    
    def filter_by_owner(self, owner: str) -> 'CollectionFilter':
        """Filter collections by owner name."""
        return self.add_filter(lambda c: c.owner and c.owner.get('name') == owner)
    
    def filter_by_min_upvotes(self, min_upvotes: int) -> 'CollectionFilter':
        """Filter collections by minimum upvotes."""
        return self.add_filter(lambda c: c.upvotes >= min_upvotes)
    
    def filter_by_item_type(self, item_type: str) -> 'CollectionFilter':
        """Filter collections containing items of specific type."""
        return self.add_filter(lambda c: any(item.item_type == item_type for item in c.items))
    
    def filter_by_item_id(self, item_id: str) -> 'CollectionFilter':
        """Filter collections containing specific item."""
        return self.add_filter(lambda c: any(item.item_id == item_id for item in c.items))
    
    def apply(self, collection: Collection) -> bool:
        """Apply all filters to a collection."""
        return all(f(collection) for f in self.filters)

class CollectionScraper(BaseScraper):
    def __init__(self, 
                 mongo_uri: str,
                 redis_uri: str,
                 rate_limit: int = 10,
                 batch_size: int = 64,
                 min_upvotes: int = 100):
        super().__init__(
            mongo_uri=mongo_uri,
            redis_uri=redis_uri,
            rate_limit=rate_limit,
            batch_size=batch_size
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.collection_filter = CollectionFilter()
        self.collection_filter.filter_by_min_upvotes(min_upvotes)

    @property
    def item_type(self) -> str:
        return "collections"

    def get_item_id(self, item: Collection) -> str:
        return item.slug

    def get_item_metadata(self, item: Collection) -> Dict[str, Any]:
        return {
            "id": item.slug,
            "title": item.title,
            "description": item.description,
            "owner": item.owner['name'] if item.owner else None,
            "last_updated": item.last_updated.isoformat() if item.last_updated else None,
            "upvotes": item.upvotes,
            "last_updated": datetime.now().isoformat()
        }

    async def list_items(self) -> Iterable[Collection]:
        """List all collections from models and datasets."""
        self.logger.info("Listing collections from models and datasets")
        
        item_ids = []
        for collection in ["models", "datasets"]:
            cursor = self.mongo_client.client[f"huggingface_scraper"][collection].find(
                {}, {"_id": 1}
            )
            async for doc in cursor:
                item_ids.append(f"{collection}/{doc['_id']}")
        
        self.logger.info(f"Found {len(item_ids)} items to check for collections")
        
        for item_id in item_ids:
            try:
                existing = await self.mongo_client.client["huggingface_scraper"]["collections"].find_one(
                    {"items": item_id}
                )
                if existing:
                    continue

                collections = self.api.list_collections(
                    item=item_id,
                    sort="upvotes",
                    limit=10
                )
                
                for collection in collections:
                    if self.collection_filter.apply(collection):
                        yield collection
                    
            except Exception as e:
                self.logger.error(f"Error fetching collections for {item_id}: {str(e)}")
                continue

    async def fetch_extended_metadata(self, item_id: str) -> Dict[str, Any]:
        """Fetch extended metadata (upvoters and full collection info) for a collection."""
        try:
            full_collection = get_collection(item_id)
            upvoters = await self._fetch_upvoters(item_id)
            
            return {
                'items': [
                    {
                        "item_id": item.item_id,
                        "item_type": item.item_type
                    } for item in full_collection.items
                ],
                'upvoters': upvoters,
                'last_updated': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error fetching extended metadata for {item_id}: {str(e)}")
            raise

    async def _fetch_upvoters(self, collection_id: str) -> List[str]:
        """Fetch upvoters for a collection using HTTP API and return a list of usernames."""
        await self.redis_client.wait_for_rate_limit("upvoters", self.rate_limit)
        
        try:
            url = f"https://huggingface.co/api/collections/{collection_id}/upvoters"
            async with self.session.get(url) as response:
                if response.status == 200:
                    upvoters = await response.json()
                    return [upvoter['user'] for upvoter in upvoters]
                return []
        except Exception as e:
            self.logger.error(f"Error fetching upvoters for {collection_id}: {str(e)}")
            return [] 