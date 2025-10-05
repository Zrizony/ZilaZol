import asyncio
import hashlib
import logging
from datetime import datetime, timezone

logger = logging.getLogger("crawler")

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def md5_bytes(b: bytes) -> str:
    h = hashlib.md5()
    h.update(b)
    return h.hexdigest()

class SemaphorePool:
    def __init__(self, n):
        self.sem = asyncio.Semaphore(n)
    async def run(self, coro):
        async with self.sem:
            return await coro
