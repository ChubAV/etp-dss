from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.domain.quarantine_service import QuarantineService


@pytest.fixture
def s3():
    return AsyncMock()


@pytest.fixture
def repo():
    return AsyncMock()


@pytest.fixture
def events():
    return AsyncMock()


@pytest.fixture
def service(s3, repo, events):
    return QuarantineService(s3=s3, repo=repo, events=events)


def _make_file():
    file = MagicMock()
    file.id = uuid4()
    file.storage_key = str(file.id)
    file.bucket = "documents-quarantine"
    file.target_bucket = "documents-private"
    file.target_key = "application/owner-id/file-id/1"
    file.checksum_sha256 = "a" * 64
    file.size_bytes = 1024
    file.owner_type = "APPLICATION"
    file.owner_id = "owner-id"
    file.correlation_id = "corr-123"
    return file


async def test_promote_copies_verifies_deletes(service, s3, repo):
    file = _make_file()
    s3.head_object = AsyncMock(return_value={"ContentLength": 1024})
    await service.promote(file)
    s3.copy_object.assert_called_once_with(
        "documents-quarantine", str(file.id), "documents-private", "application/owner-id/file-id/1"
    )
    s3.head_object.assert_called_once_with("documents-private", "application/owner-id/file-id/1")
    s3.delete_object.assert_called_once_with("documents-quarantine", str(file.id))
    repo.update_after_promotion.assert_called_once_with(file.id)


async def test_promote_publishes_events(service, s3, repo, events):
    file = _make_file()
    s3.head_object = AsyncMock(return_value={"ContentLength": 1024})
    await service.promote(file)
    assert events.publish.call_count == 2
    event_types = [call.kwargs["event_type"] for call in events.publish.call_args_list]
    assert "FILE_AV_PASSED" in event_types
    assert "FILE_MOVED_TO_STORAGE" in event_types


async def test_promote_aborts_on_head_object_failure(service, s3, repo):
    file = _make_file()
    s3.head_object = AsyncMock(side_effect=Exception("S3 error"))
    with pytest.raises(Exception, match="S3 error"):
        await service.promote(file)
    s3.copy_object.assert_called_once()
    s3.delete_object.assert_not_called()
    repo.update_after_promotion.assert_not_called()


async def test_promote_aborts_on_size_mismatch(service, s3, repo):
    file = _make_file()
    s3.head_object = AsyncMock(return_value={"ContentLength": 999})
    with pytest.raises(RuntimeError, match="Size mismatch"):
        await service.promote(file)
    s3.copy_object.assert_called_once()
    s3.delete_object.assert_not_called()
    repo.update_after_promotion.assert_not_called()
