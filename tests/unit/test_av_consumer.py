import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.infrastructure.av_consumer import AVResultConsumer


@pytest.fixture
def repo():
    return AsyncMock()


@pytest.fixture
def quarantine():
    return AsyncMock()


@pytest.fixture
def events():
    return AsyncMock()


@pytest.fixture
def consumer(repo, quarantine, events):
    return AVResultConsumer(repo=repo, quarantine_service=quarantine, events=events)


def _make_result_message(file_id, status="CLEAN"):
    threats: list[str] = [] if status == "CLEAN" else ["EICAR"]
    payload = {
        "task_id": str(uuid4()),
        "file_id": str(file_id),
        "status": status,
        "engine": "ClamAV 1.3.1",
        "scanned_at": datetime.now(UTC).isoformat(),
        "details": {
            "signatures_checked": 100,
            "scan_duration_ms": 50,
            "threats_found": threats,
        },
    }
    return json.dumps(payload).encode()


async def test_handle_clean_result_promotes(consumer, repo, quarantine):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    repo.get_by_id = AsyncMock(return_value=file)
    await consumer.handle_message(_make_result_message(file_id, "CLEAN"))
    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "CLEAN"
    quarantine.promote.assert_called_once_with(file)


async def test_handle_infected_publishes_event(consumer, repo, quarantine, events):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    file.owner_type = "APPLICATION"
    file.owner_id = "owner-123"
    file.correlation_id = "corr-123"
    repo.get_by_id = AsyncMock(return_value=file)
    await consumer.handle_message(_make_result_message(file_id, "INFECTED"))
    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "INFECTED"
    quarantine.promote.assert_not_called()
    events.publish.assert_called_once()
    assert events.publish.call_args.kwargs["event_type"] == "FILE_AV_FAILED"


async def test_handle_skips_already_clean(consumer, repo, quarantine):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "CLEAN"
    repo.get_by_id = AsyncMock(return_value=file)
    await consumer.handle_message(_make_result_message(file_id, "CLEAN"))
    repo.update_av_status.assert_not_called()
    quarantine.promote.assert_not_called()


async def test_handle_error_result(consumer, repo, quarantine, events):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    repo.get_by_id = AsyncMock(return_value=file)
    await consumer.handle_message(_make_result_message(file_id, "ERROR"))
    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "ERROR"
    quarantine.promote.assert_not_called()
