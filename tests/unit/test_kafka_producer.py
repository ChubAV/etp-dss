from unittest.mock import AsyncMock

import pytest

from app.infrastructure.kafka_producer import KafkaProducer


@pytest.fixture
def producer():
    return KafkaProducer(bootstrap_servers="localhost:9092")


async def test_send_message(producer):
    mock_aiokafka = AsyncMock()
    producer._producer = mock_aiokafka
    producer._started = True
    await producer.send(topic="test-topic", key="test-key", value=b'{"foo": "bar"}')
    mock_aiokafka.send_and_wait.assert_called_once_with(
        "test-topic", key=b"test-key", value=b'{"foo": "bar"}'
    )


async def test_send_raises_when_not_started(producer):
    with pytest.raises(RuntimeError, match="not started"):
        await producer.send("topic", "key", b"value")
