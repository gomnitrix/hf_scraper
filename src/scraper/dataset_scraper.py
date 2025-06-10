from typing import List, Dict, Any, AsyncIterable
from .base_scraper import BaseScraper
from huggingface_hub import DatasetInfo, ModelCard
from datetime import datetime
import asyncio

class DatasetScraper(BaseScraper):
    FILTERED_TAG_PREFIXES = {
        'af', 'am', 'ar', 'az', 'be', 'bg', 'bn', 'bs', 'ca', 'ceb',
        'co', 'cs', 'cy', 'da', 'de', 'el', 'en', 'eo', 'es', 'et',
        'eu', 'fa', 'fi', 'fr', 'fy', 'ga', 'gd', 'gl', 'gu', 'ha',
        'haw', 'he', 'hi', 'hmn', 'hr', 'ht', 'hu', 'hy', 'id', 'ig',
        'is', 'it', 'iw', 'ja', 'jw', 'ka', 'kk', 'km', 'kn', 'ko',
        'ku', 'ky', 'la', 'lb', 'lo', 'lt', 'lv', 'mg', 'mi', 'mk',
        'ml', 'mn', 'mr', 'ms', 'mt', 'my', 'ne', 'nl', 'no', 'ny',
        'or', 'pa', 'pl', 'ps', 'pt', 'ro', 'ru', 'rw', 'sd', 'si',
        'sk', 'sl', 'sm', 'sn', 'so', 'sq', 'sr', 'st', 'su', 'sv',
        'sw', 'ta', 'te', 'tg', 'th', 'tk', 'tl', 'tr', 'tt', 'ug',
        'uk', 'ur', 'uz', 'vi', 'xh', 'yi', 'yo', 'zh', 'zu',
        'language:',
        'region:'
    }

    EXPANDED_FIELDS = [
        "author",
        "cardData", 
        "createdAt", 
        "disabled", 
        "downloads", 
        "downloadsAllTime", 
        "lastModified", 
        "likes", 
        "private", 
        "tags"
    ]

    @property
    def item_type(self) -> str:
        return "datasets"

    def get_item_id(self, item: DatasetInfo) -> str:
        return item.id

    def _filter_tags(self, tags: List[str]) -> List[str]:
        """Filter out unwanted tags."""
        return [
            tag for tag in tags
            if not any(tag.startswith(prefix) for prefix in self.FILTERED_TAG_PREFIXES)
        ]

    def get_item_metadata(self, item: DatasetInfo) -> Dict[str, Any]:
        dataset_info = item
        return {
            # basic info
            "id": dataset_info.id,  
            "author": dataset_info.author, 
            
            # time info
            "created_at": dataset_info.created_at.isoformat() if dataset_info.created_at else None, 
            "last_modified": dataset_info.last_modified.isoformat() if dataset_info.last_modified else None, 
            
            # stats info
            "downloads": {
                "current": dataset_info.downloads if dataset_info.downloads else None, 
                "all_time": dataset_info.downloads_all_time if dataset_info.downloads_all_time else None, 
            },
            "likes": dataset_info.likes if dataset_info.likes else None, 
            
            # dataset card info
            "card_data": {
                "annotations_creators": dataset_info.card_data.get("annotations_creators") if dataset_info.card_data else None,
                "language_creators": dataset_info.card_data.get("language_creators") if dataset_info.card_data else None,
                "size_categories": dataset_info.card_data.get("size_categories") if dataset_info.card_data else None,
                "source_datasets": dataset_info.card_data.get("source_datasets") if dataset_info.card_data else None,
                "task_categories": dataset_info.card_data.get("task_categories") if dataset_info.card_data else None,
                "task_ids": dataset_info.card_data.get("task_ids") if dataset_info.card_data else None,
                "paperswithcode_id": dataset_info.card_data.get("paperswithcode_id") if dataset_info.card_data else None,
            } if dataset_info.card_data else None,
            
            # tags and categories
            "tags": self._filter_tags(dataset_info.tags), 
            
            # status info
            "status": {
                "private": dataset_info.private, 
                "disabled": dataset_info.disabled, 
                "gated": dataset_info.gated, 
            },
        }
    

    async def list_items(self) -> AsyncIterable[DatasetInfo]:
        """List all available datasets."""
        if self.resource_ids:
            # Create an async generator for each resource ID
            async def search_generator():
                for dataset_id in self.resource_ids:
                    async for dataset in self._search_datasets(dataset_id):
                        yield dataset
            
            dataset_iterator = search_generator()
        else:
            # Use list all datasets if no resource IDs specified
            dataset_iterator = self._list_all_datasets()

        # Apply tags filtering only if tags are specified
        if self.tags:
            async for dataset in dataset_iterator:
                if any(x in set(dataset.tags) for x in self.tags):
                    yield dataset
        else:
            # Return all datasets if no tags specified
            async for dataset in dataset_iterator:
                yield dataset

    async def _list_all_datasets(self) -> AsyncIterable[DatasetInfo]:
        """List all datasets with lazy loading."""
        datasets = self.api.list_datasets(
            limit=self.limit,
            expand=self.EXPANDED_FIELDS
        )
        for dataset in datasets:
            yield dataset
    
    async def _search_datasets(self, dataset_id: str) -> AsyncIterable[DatasetInfo]:
        """Search datasets by ID with lazy loading."""
        await self.redis_client.wait_for_rate_limit("search_datasets", self.rate_limit)

        datasets = self.api.list_datasets(
            search=dataset_id,
            expand=self.EXPANDED_FIELDS
        )
        for dataset in datasets:
            if dataset.id == dataset_id:
                yield dataset

    async def fetch_extended_metadata(self, item_id: str) -> Dict[str, Any]:
        """Fetch extended metadata (README and likers) for an item."""
        
        try:
            # readme_content = await self._fetch_readme(item_id)
            likers, contributors = await asyncio.gather(
                self._fetch_likers(item_id),
                self._fetch_contributors(item_id)
            )

            return {
                # 'readme': readme_content,
                'likers': likers,
                'contributors': contributors,
                'last_updated': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error fetching extended metadata for {item_id}: {str(e)}")
            raise

    # TODO: Implement this for datasets
    async def _fetch_readme(self, item_id: str) -> str:
        """Fetch README content for an item."""
        await self.redis_client.wait_for_rate_limit("readme", self.rate_limit)
        
        try:
            card = ModelCard.load(item_id)
            return card.text
        except Exception as e:
            self.logger.error(f"Error fetching README for {item_id}: {str(e)}")
            return ""

    async def _fetch_likers(self, item_id: str) -> List[str]:
        """Fetch likers for an item using HTTP API and return a list of usernames."""
        await self.redis_client.wait_for_rate_limit("likers", self.rate_limit)
        
        try:
            url = f"https://huggingface.co/api/{self.item_type}/{item_id}/likers"
            async with self.session.get(url) as response:
                if response.status == 200:
                    likers = await response.json()
                    return [liker['user'] for liker in likers]
                return []
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching likers for dataset: {item_id}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching likers for {item_id}: {str(e)}")
            return []
        
    async def _fetch_contributors(self, item_id: str) -> List[str]:
        """Fetch contributors for an item using HTTP API and return a list of usernames."""
        await self.redis_client.wait_for_rate_limit("contributors", self.rate_limit)
        
        try:
            unique_contributors = list(dict.fromkeys(
                author
                for commit in self.api.list_repo_commits(item_id, repo_type='dataset')
                for author in commit.authors
            ))
            return unique_contributors
        except Exception as e:
            self.logger.error(f"Error fetching contributors for {item_id}: {str(e)}")
            return []