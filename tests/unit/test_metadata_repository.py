import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.db_models import File
from app.storage.metadata_repository import MetadataRepository


@pytest.fixture
def session():
    s = AsyncMock()
    s.add = MagicMock()
    s.flush = AsyncMock()
    s.commit = AsyncMock()
    return s


@pytest.fixture
def repo(session):
    return MetadataRepository(session)


async def test_create_file(repo, session):
    file = File(
        original_name="test.pdf",
        storage_key="quarantine/test",
        bucket="documents-quarantine",
        content_type="application/pdf",
        size_bytes=1024,
        checksum_sha256="abc",
        checksum_gost="def",
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        uploaded_by=uuid.uuid4(),
    )
    await repo.create(file)
    session.add.assert_called_once_with(file)
    session.flush.assert_called_once()


async def test_get_by_id_filters_deleted(repo, session):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=mock_result)
    result = await repo.get_by_id(uuid.uuid4())
    assert result is None
    session.execute.assert_called_once()


async def test_update_av_status(repo, session):
    await repo.update_av_status(
        file_id=uuid.uuid4(),
        av_status="CLEAN",
        av_scanned_at=datetime.now(UTC),
        av_engine="ClamAV 1.3.1",
        av_report={"threats_found": []},
    )
    session.execute.assert_called_once()
    session.flush.assert_called()


async def test_update_after_promotion(repo, session):
    file = MagicMock()
    file.target_bucket = "documents-private"
    file.target_key = "application/owner-id/file-id/1"

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = file
    session.execute = AsyncMock(return_value=mock_result)

    await repo.update_after_promotion(file_id=uuid.uuid4())
    assert session.execute.call_count >= 1
    session.flush.assert_called()
