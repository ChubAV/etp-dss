from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.infrastructure.events import EventProducer


@pytest.fixture
def kafka():
    return AsyncMock()


@pytest.fixture
def producer(kafka):
    return EventProducer(kafka=kafka, topic="documents.events")


async def test_publish_lifecycle_event(producer, kafka):
    file_id = uuid4()
    await producer.publish(
        event_type="FILE_AV_PASSED",
        file_id=file_id,
        owner_type="APPLICATION",
        owner_id="owner-123",
        correlation_id="corr-123",
        details={"engine": "ClamAV 1.3.1"},
    )
    kafka.send.assert_called_once()
    call_args = kafka.send.call_args
    assert call_args.args[0] == "documents.events"
    assert call_args.args[1] == str(file_id)
    assert b"FILE_AV_PASSED" in call_args.args[2]
