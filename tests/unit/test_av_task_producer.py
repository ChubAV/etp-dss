from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.infrastructure.av_task_producer import AVTaskProducer


@pytest.fixture
def kafka():
    return AsyncMock()


@pytest.fixture
def producer(kafka):
    return AVTaskProducer(kafka=kafka, topic="documents.av.scan.request")


async def test_publish_scan_request(producer, kafka):
    file_id = uuid4()
    await producer.publish(
        file_id=file_id,
        storage_key=str(file_id),
        bucket="documents-quarantine",
        content_type="application/pdf",
        size_bytes=1024,
        correlation_id="corr-123",
    )
    kafka.send.assert_called_once()
    call_args = kafka.send.call_args
    assert call_args.args[0] == "documents.av.scan.request"
    assert call_args.args[1] == str(file_id)
    assert b"documents-quarantine" in call_args.args[2]
