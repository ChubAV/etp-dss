import structlog
from redis.asyncio import Redis

logger = structlog.get_logger()


class CacheClient:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def blacklist_token(self, jti: str, ttl_seconds: int) -> None:
        await self._redis.set(f"token:blacklist:{jti}", "1", ex=ttl_seconds)

    async def is_token_blacklisted(self, jti: str) -> bool:
        try:
            return await self._redis.exists(f"token:blacklist:{jti}") > 0
        except Exception:
            logger.warning("redis_unavailable_fail_closed", jti=jti)
            return True  # fail-closed

    async def cache_presigned_url(
        self, file_id: str, disposition: str, url: str, ttl_seconds: int
    ) -> None:
        try:
            await self._redis.set(f"presigned:{file_id}:{disposition}", url, ex=ttl_seconds)
        except Exception:
            logger.warning("redis_cache_write_failed", file_id=file_id)

    async def get_cached_presigned_url(self, file_id: str, disposition: str) -> str | None:
        try:
            return await self._redis.get(f"presigned:{file_id}:{disposition}")
        except Exception:
            logger.warning("redis_cache_read_failed", file_id=file_id)
            return None
