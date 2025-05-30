from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Iterable
from huggingface_hub import HfApi
from rich.progress import Progress, TaskID
from rich.console import Console
import logging
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.redis_client import RedisClient
from utils.mongodb import MongoDBClient

class BaseScraper(ABC):
    def __init__(self, 
                 mongo_uri: str,
                 redis_uri: str,
                 limit: Optional[int] = None, 
                 tags: Optional[List[str]] = None,
                 rate_limit: int = 10,
                 batch_size: int = 64):
        self.api = HfApi()
        self.limit = limit
        self.tags = tags
        self.rate_limit = rate_limit
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)
        self.console = Console()
        
        # Initialize clients
        self.mongo_client = MongoDBClient(mongo_uri)
        self.redis_client = RedisClient(redis_uri)

        self._basic_stage = False
        self._extended_stage = False

    @property
    @abstractmethod
    def item_type(self) -> str:
        """Return the type of item being scraped (models/datasets)."""
        pass

    @abstractmethod
    def get_item_id(self, item: Any) -> str:
        """Get the item ID for an item."""
        pass

    @abstractmethod
    def get_item_metadata(self, item: Any) -> Dict[str, Any]:
        """Get the metadata for an item."""
        pass

    @abstractmethod
    def list_items(self) -> Iterable[Any]:
        """List all available items of this type."""
        pass

    def skip_basic_stage(self) -> None:
        """Skip the basic stage of scraping."""
        self._basic_stage = True
    
    def skip_extended_stage(self) -> None:
        """Skip the extended stage of scraping."""
        self._extended_stage = True

    async def scrape(self, progress: Progress, basic_task: TaskID) -> None:
        """Scrape metadata for all items."""
        # Process items in batches
        batch = []
        async for item in self.list_items():
            batch.append(item)
            if len(batch) >= self.batch_size:
                await self._batch_record(batch)
                batch = []
        
        # Process remaining items
        if batch:
            await self._batch_record(batch)
            
        if not self._basic_stage:
            self._basic_stage = True
            progress.update(basic_task, description=f"Scraping {self.item_type} base metadata completed", completed=1)
            progress.stop_task(basic_task)
            progress.remove_task(basic_task)
            self.console.print(f"[green]✓[/green] Scraping {self.item_type} basic metadata completed")

    async def _batch_record(self, batch: List[Any]) -> None:
        """Process a batch of items."""
        # Get item IDs and metadata
        item_metadata = [self.get_item_metadata(item) for item in batch]
        try:
            # Save basic metadata
            await self.mongo_client.bulk_upsert_basic_metadata(self.item_type, item_metadata)
            await self._record_extended_tasks(batch)
        except Exception as e:
            self.logger.error(f"Error processing batch: {str(e)}")
    
    async def _record_extended_tasks(self, batch: List[Any]) -> None:
        item_ids = [self.get_item_id(item) for item in batch]
        for item_id in item_ids:
            try:
                await self.redis_client.create_task(item_id, self.item_type)     
            except Exception as e:
                self.logger.error(f"Error creating task for {item_id}: {str(e)}")

    async def process_extended_tasks(self, progress: Progress, extended_task: TaskID) -> None:
        """Process extended metadata tasks from Redis queue using thread pool."""
        while True:
            tasks = []
            for _ in range(self.rate_limit//2):
                task_data = await self.redis_client.get_task(self.item_type)
                if not task_data:
                    break
                tasks.append(task_data)
                
            if not tasks and self._basic_stage:
                self.logger.info("Extended stage is completed")
                self._extended_stage = True
                break
            elif not tasks:
                self.logger.info("No more tasks to process, waiting for 2 seconds")
                await asyncio.sleep(2)
                self.logger.info("Continuing to check for tasks")
                continue
                
            await asyncio.gather(
                *[self._process_single_task(task_data) for task_data in tasks]
            )
            progress.advance(extended_task, len(tasks))
        
        progress.update(extended_task, description=f"Scraping {self.item_type} extended metadata completed", completed=1)
        progress.stop_task(extended_task)
        progress.remove_task(extended_task)
        self.console.print(f"[green]✓[/green] Scraping {self.item_type} extended metadata completed")

    async def _process_single_task(self, task: Dict[str, Any]) -> None:
        """Process a single extended metadata task."""
        try:
            # Fetch extended metadata
            extended_metadata = await self.fetch_extended_metadata(task["iid"])
            
            # Update MongoDB document
            await self.mongo_client.upsert_extended_metadata(
                task["type"],
                task["iid"],
                extended_metadata
            )
            
        except Exception as e:
            self.logger.error(
                f"Error processing {self.item_type} {task['iid']}: {str(e)} "
                f"(Attempt {task.get('retry_count', 0) + 1}/3)"
            )
            
            if task.get("retry_count", 0) < 3:
                task["retry_count"] = task.get("retry_count", 0) + 1
                await self.redis_client.create_task(task["iid"], task["type"])
            else:
                self.logger.error(f"Failed to scrape {self.item_type} {task['iid']} after 3 attempts")

    # @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    @abstractmethod
    async def fetch_extended_metadata(self, item_id: str) -> Dict[str, Any]:
        """Fetch extended metadata for an item."""
        pass
    # TODO: Add contributors