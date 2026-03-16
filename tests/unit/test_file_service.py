import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.domain.exceptions import FileNotFoundError as DSSFileNotFoundError
from app.domain.file_service import FileService


@pytest.fixture
def deps():
    repo = AsyncMock()
    s3 = AsyncMock()
    cache = AsyncMock()
    cache.get_cached_presigned_url = AsyncMock(return_value=None)
    settings = MagicMock()
    settings.s3_bucket_private = "documents-private"
    settings.s3_bucket_public = "documents-public"
    settings.max_file_size_mb = 20
    settings.allowed_content_types = ["application/pdf", "image/png"]
    settings.presigned_url_ttl_seconds = 300
    settings.public_presigned_url_ttl_seconds = 3600
    return repo, s3, cache, settings


@pytest.fixture
def service(deps):
    return FileService(*deps)


async def test_upload_creates_file_record(service, deps):
    repo, s3, cache, settings = deps
    s3.upload_object = AsyncMock(return_value="v1")
    repo.create = AsyncMock(side_effect=lambda f: f)

    async def chunks():
        yield b"file content"

    with patch("app.domain.file_service.compute_hashes", return_value=("sha256hex", "gosthex")):
        result = await service.upload(
            file_stream=chunks(), file_name="test.pdf",
            content_type="application/pdf", size_bytes=1024,
            owner_type="LOT", owner_id=uuid.uuid4(),
            visibility="PRIVATE", uploaded_by=uuid.uuid4(),
        )

    assert result.original_name == "test.pdf"
    assert result.checksum_sha256 == "sha256hex"
    repo.create.assert_called_once()
    s3.upload_object.assert_called_once()


async def test_upload_rejects_invalid_content_type(service):
    async def chunks():
        yield b"data"

    with pytest.raises(ValueError, match="not allowed"):
        await service.upload(
            file_stream=chunks(), file_name="test.zip",
            content_type="application/zip", size_bytes=1024,
            owner_type="LOT", owner_id=uuid.uuid4(),
            visibility="PRIVATE", uploaded_by=uuid.uuid4(),
        )


async def test_get_file_not_found(service, deps):
    deps[0].get_by_id.return_value = None
    with pytest.raises(DSSFileNotFoundError):
        await service.get_file(uuid.uuid4())


async def test_presigned_url_uses_disposition_cache_key(service, deps):
    repo, s3, cache, settings = deps
    file = MagicMock()
    file.av_status = "CLEAN"
    file.visibility = "PRIVATE"
    file.bucket = "documents-private"
    file.storage_key = "lot/uuid/file/1"
    file.id = uuid.uuid4()
    repo.get_by_id.return_value = file
    s3.generate_presigned_url = AsyncMock(return_value="https://s3/url")

    await service.generate_presigned_url(file.id, disposition="attachment")
    cache.get_cached_presigned_url.assert_called_with(str(file.id), "attachment")
