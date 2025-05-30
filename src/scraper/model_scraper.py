from typing import List, Dict, Any, Iterable
from .base_scraper import BaseScraper
from huggingface_hub import ModelInfo, ModelCard
import aiohttp
from datetime import datetime
import asyncio

class ModelScraper(BaseScraper):
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
        'base_model:', 'license:', 'region:', 'language:', 'dataset:'
    }

    @property
    def item_type(self) -> str:
        return "models"

    def get_item_id(self, item: ModelInfo) -> str:
        return item.id

    def _filter_tags(self, tags: List[str]) -> List[str]:
        """Filter out unwanted tags."""
        return [
            tag for tag in tags
            if not any(tag.startswith(prefix) for prefix in self.FILTERED_TAG_PREFIXES)
        ]

    def get_item_metadata(self, item: ModelInfo) -> Dict[str, Any]:
        model_info = item
        return {
            # basic info
            "id": model_info.id,  
            "author": model_info.author, 
            
            # time info
            "created_at": model_info.created_at.isoformat() if model_info.created_at else None, 
            "last_modified": model_info.last_modified.isoformat() if model_info.last_modified else None, 
            
            # stats info
            "downloads": {
                "current": model_info.downloads if model_info.downloads else None, 
                "all_time": model_info.downloads_all_time if model_info.downloads_all_time else None, 
            },
            "likes": model_info.likes if model_info.likes else None, 
            
            # model card info
            "card_data": {
                "base_model": model_info.card_data.get("base_model") if model_info.card_data else None,
                "datasets": model_info.card_data.get("datasets") if model_info.card_data else None,
                "license": {
                    "name": model_info.card_data.get("license") if model_info.card_data else None, 
                    "link": model_info.card_data.get("license_link") if model_info.card_data else None, 
                },
                "pipeline_tag": model_info.card_data.get("pipeline_tag") if model_info.card_data else None, 
                "base_model_relation": model_info.card_data.get("base_model_relation") if model_info.card_data else None, 
            } if model_info.card_data else None,
            
            # tags and categories
            "tags": self._filter_tags(model_info.tags), 
            "library_name": model_info.library_name, 
            
            # status info
            "status": {
                "private": model_info.private, 
                "disabled": model_info.disabled, 
                "gated": model_info.gated, 
            },
        }

    async def list_items(self) -> Iterable[ModelInfo]:
        """List all available models."""
        model_iterator = self.api.list_models(
            limit=self.limit,
            expand=[
                "author",
                "cardData",
                "createdAt",
                "disabled",
                "downloads",
                "downloadsAllTime",
                "inference",
                "lastModified",
                "library_name",
                "likes",
                "pipeline_tag",
                "private",
                "tags"
            ]
        )
        for model in model_iterator:
            if any(x in set(model.tags) for x in self.tags):
                yield model
    
    async def fetch_extended_metadata(self, item_id: str) -> Dict[str, Any]:
        """Fetch extended metadata (README and likers) for an item."""
        
        try:
            #readme_content = await self._fetch_readme(item_id)
            # Fetch likers and contributors in parallel
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
            async with aiohttp.ClientSession() as session:
                url = f"https://huggingface.co/api/{self.item_type}/{item_id}/likers"
                async with session.get(url) as response:
                    if response.status == 200:
                        likers = await response.json()
                        return [liker['user'] for liker in likers]
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
                for commit in self.api.list_repo_commits(item_id, repo_type='model')
                for author in commit.authors
            ))
            return unique_contributors
        except Exception as e:
            self.logger.error(f"Error fetching contributors for {item_id}: {str(e)}")
            return []