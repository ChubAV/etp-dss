from unittest.mock import AsyncMock

import pytest

from app.storage.cache_client import CacheClient


@pytest.fixture
def redis_mock():
    mock = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock()
    mock.exists = AsyncMock(return_value=0)
    return mock


@pytest.fixture
def cache(redis_mock):
    return CacheClient(redis_mock)


async def test_blacklist_token(cache, redis_mock):
    await cache.blacklist_token("jti-123", ttl_seconds=600)
    redis_mock.set.assert_called_once_with("token:blacklist:jti-123", "1", ex=600)


async def test_is_token_blacklisted_false(cache, redis_mock):
    redis_mock.exists.return_value = 0
    assert await cache.is_token_blacklisted("jti-123") is False


async def test_is_token_blacklisted_true(cache, redis_mock):
    redis_mock.exists.return_value = 1
    assert await cache.is_token_blacklisted("jti-123") is True


async def test_is_token_blacklisted_redis_down_returns_true(cache, redis_mock):
    """Fail-closed: if Redis is down, treat token as blacklisted."""
    redis_mock.exists.side_effect = ConnectionError("Redis unavailable")
    assert await cache.is_token_blacklisted("jti-123") is True


async def test_cache_presigned_url_includes_disposition(cache, redis_mock):
    await cache.cache_presigned_url("file-id", "inline", "https://s3/presigned", ttl_seconds=240)
    redis_mock.set.assert_called_once_with(
        "presigned:file-id:inline", "https://s3/presigned", ex=240
    )


async def test_get_cached_presigned_url_by_disposition(cache, redis_mock):
    redis_mock.get.return_value = "https://s3/presigned"
    result = await cache.get_cached_presigned_url("file-id", "inline")
    assert result == "https://s3/presigned"
    redis_mock.get.assert_called_once_with("presigned:file-id:inline")
