import aioredis
import time
from typing import Optional, Dict, Any
import json
from datetime import datetime
import asyncio

class RedisClient:
    def __init__(self, redis_uri: str):
        self.redis_uri = redis_uri
        self.client = None
        self.rate_limit_key = "rate_limit"
        self.rate_limit_window = 60  # 1 minute window
        self.script = """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local start = now - window

            redis.call("ZREMRANGEBYSCORE", key, 0, start)
            local count = redis.call("ZCARD", key)
            if count >= limit then
                return 1
            end
            redis.call("ZADD", key, now, tostring(now))
            return 0
        """

    async def connect(self):
        """Connect to Redis."""
        if self.client is None:
            self.client = await aioredis.from_url(self.redis_uri)
            self.script_sha = await self.client.script_load(self.script)

    async def is_rate_limited(self, key: str, limit: int) -> bool:
        """Check if a request should be rate limited using atomic operations."""
        await self.connect()
        is_limited = await self.client.evalsha(
            self.script_sha, 
            1, 
            key, 
            int(time.time()), 
            self.rate_limit_window, 
            limit
        )
        return is_limited == 1

    async def wait_for_rate_limit(self, key: str, limit: int) -> None:
        """Wait until rate limit allows the request."""
        key = f"{self.rate_limit_key}:{key}"
        while await self.is_rate_limited(key, limit):
            await asyncio.sleep(1)

    async def create_task(self, item_id: str, task_type: str) -> str:
        """Create a new task in Redis."""
        await self.connect()
        task_id = f"{task_type}:{item_id}"
        task_data = {
            "tid": task_id,
            "iid": item_id,
            "type": task_type,
            "status": "pending",
            "retry_count": 0,
            "created_at": datetime.now().isoformat()
        }
        await self.client.lpush(f"tasks_{task_type}", json.dumps(task_data))
        return task_id

    async def get_task(self, task_type: str) -> Optional[Dict[str, Any]]:
        """Get next task from queue."""
        await self.connect()
        task_data = await self.client.rpop(f"tasks_{task_type}")
        if task_data:
            return json.loads(task_data)
        return None 