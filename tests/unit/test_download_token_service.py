import uuid
from unittest.mock import AsyncMock

import pytest
from app.domain.download_token_service import DownloadTokenService


@pytest.fixture
def cache():
    mock = AsyncMock()
    mock.is_token_blacklisted = AsyncMock(return_value=False)
    mock.blacklist_token = AsyncMock()
    return mock


@pytest.fixture
def service(cache):
    return DownloadTokenService(
        secret="test-secret-256bit-long-enough-key!", algorithm="HS256",
        max_ttl_seconds=600, cache=cache,
    )


def test_generate_returns_string(service):
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    assert isinstance(token, str)


async def test_validate_returns_payload(service):
    file_id = uuid.uuid4()
    token = service.generate(
        file_id=file_id, user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    payload = await service.validate(token)
    assert payload["file_id"] == str(file_id)
    assert payload["disposition"] == "inline"


async def test_ttl_capped_at_max(service):
    """Token with expires_in > max_ttl should have exp capped."""
    import jwt as pyjwt
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=9999,
    )
    payload = pyjwt.decode(token, "test-secret-256bit-long-enough-key!", algorithms=["HS256"])
    actual_ttl = payload["exp"] - payload["iat"]
    assert actual_ttl <= 600


async def test_consume_blacklists(service, cache):
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    await service.consume(token)
    cache.blacklist_token.assert_called_once()
