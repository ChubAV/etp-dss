from datetime import UTC, datetime
from uuid import UUID

import structlog

from app.domain.kafka_schemas import FileLifecycleEvent
from app.infrastructure.kafka_producer import KafkaProducer

logger = structlog.get_logger()


class EventProducer:
    def __init__(self, kafka: KafkaProducer, topic: str) -> None:
        self._kafka = kafka
        self._topic = topic

    async def publish(
        self,
        event_type: str,
        file_id: UUID,
        owner_type: str,
        owner_id: str,
        correlation_id: str,
        details: dict,
        actor_id: str | None = None,
    ) -> None:
        event = FileLifecycleEvent(
            event_type=event_type,
            file_id=file_id,
            owner_type=owner_type,
            owner_id=owner_id,
            timestamp=datetime.now(UTC),
            correlation_id=correlation_id,
            actor_id=actor_id,
            details=details,
        )
        await self._kafka.send(self._topic, str(file_id), event.model_dump_json().encode())
        logger.info("lifecycle_event_published", event_type=event_type, file_id=str(file_id))
