import uuid
from unittest.mock import AsyncMock

import pytest

from app.domain.upload_token_service import UploadTokenService


@pytest.fixture
def cache():
    mock = AsyncMock()
    mock.is_token_blacklisted = AsyncMock(return_value=False)
    mock.blacklist_token = AsyncMock()
    return mock


@pytest.fixture
def service(cache):
    return UploadTokenService(
        secret="test-secret-256bit-long-enough-key!",
        algorithm="HS256",
        ttl_seconds=600,
        cache=cache,
    )


def test_generate_returns_string(service):
    token = service.generate(
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        visibility="PUBLIC",
        file_name="test.pdf",
        content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024,
        uploaded_by=uuid.uuid4(),
    )
    assert isinstance(token, str) and len(token) > 0


async def test_validate_returns_payload(service):
    owner_id = uuid.uuid4()
    uploaded_by = uuid.uuid4()
    token = service.generate(
        owner_type="LOT",
        owner_id=owner_id,
        visibility="PUBLIC",
        file_name="test.pdf",
        content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024,
        uploaded_by=uploaded_by,
    )
    payload = await service.validate(token)
    assert payload["owner_type"] == "LOT"
    assert payload["owner_id"] == str(owner_id)
    assert payload["uploaded_by"] == str(uploaded_by)


async def test_validate_blacklisted_raises(service, cache):
    cache.is_token_blacklisted.return_value = True
    token = service.generate(
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        visibility="PRIVATE",
        file_name="t.pdf",
        content_type="application/pdf",
        max_size_bytes=1024,
        uploaded_by=uuid.uuid4(),
    )
    with pytest.raises(ValueError, match="blacklisted"):
        await service.validate(token)


async def test_consume_blacklists(service, cache):
    token = service.generate(
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        visibility="PRIVATE",
        file_name="t.pdf",
        content_type="application/pdf",
        max_size_bytes=1024,
        uploaded_by=uuid.uuid4(),
    )
    await service.consume(token)
    cache.blacklist_token.assert_called_once()
