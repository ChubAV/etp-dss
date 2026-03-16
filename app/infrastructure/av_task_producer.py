from datetime import UTC, datetime
from uuid import UUID, uuid4

import structlog

from app.domain.kafka_schemas import AVScanRequest
from app.infrastructure.kafka_producer import KafkaProducer

logger = structlog.get_logger()


class AVTaskProducer:
    def __init__(self, kafka: KafkaProducer, topic: str) -> None:
        self._kafka = kafka
        self._topic = topic

    async def publish(
        self,
        file_id: UUID,
        storage_key: str,
        bucket: str,
        content_type: str,
        size_bytes: int,
        correlation_id: str,
    ) -> None:
        request = AVScanRequest(
            task_id=uuid4(),
            file_id=file_id,
            storage_key=storage_key,
            bucket=bucket,
            content_type=content_type,
            size_bytes=size_bytes,
            correlation_id=correlation_id,
            requested_at=datetime.now(UTC),
        )
        await self._kafka.send(
            self._topic,
            str(file_id),
            request.model_dump_json().encode(),
        )
        logger.info("av_scan_request_published", file_id=str(file_id), task_id=str(request.task_id))
