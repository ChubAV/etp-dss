from unittest.mock import AsyncMock, MagicMock

import pytest

from app.storage.s3_client import S3Client


@pytest.fixture
def s3_client():
    return S3Client(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        region="us-east-1",
    )


async def test_upload_object(s3_client):
    mock_s3 = AsyncMock()
    mock_s3.put_object = AsyncMock(return_value={"VersionId": "v1"})

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    version_id = await s3_client.upload_object(
        bucket="test-bucket", key="test-key", body=b"test-data", content_type="application/pdf",
    )
    assert version_id == "v1"
    mock_s3.put_object.assert_called_once()


async def test_generate_presigned_url(s3_client):
    mock_s3 = AsyncMock()
    mock_s3.generate_presigned_url = AsyncMock(return_value="https://s3/presigned")

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    url = await s3_client.generate_presigned_url(
        bucket="test-bucket", key="test-key", expires_in=300,
    )
    assert url == "https://s3/presigned"
