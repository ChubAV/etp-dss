"""AV Worker — standalone process: consume scan requests, scan via ClamAV, publish results."""

import asyncio
from datetime import UTC, datetime

import structlog

from app.config import Settings
from app.domain.kafka_schemas import AVScanDetails, AVScanRequest, AVScanResult
from app.domain.scanner import ScanResult
from app.infrastructure.clamav_scanner import ClamAVScanner
from app.infrastructure.kafka_producer import KafkaProducer
from app.storage.s3_client import S3Client

logger = structlog.get_logger()
RETRY_DELAYS = [5, 15, 30]
MAX_CONCURRENT_SCANS = 3


async def process_scan_request(raw: bytes, s3, scanner, producer, result_topic: str) -> None:
    request = AVScanRequest.model_validate_json(raw)
    file_id = request.file_id
    logger.info("av_scan_started", file_id=str(file_id))
    data = await s3.get_object(request.bucket, request.storage_key)
    scan_result: ScanResult | None = None
    for attempt in range(len(RETRY_DELAYS)):
        try:
            scan_result = await scanner.scan(data)
            break
        except Exception:
            logger.warning(
                "av_scan_retry",
                file_id=str(file_id),
                attempt=attempt + 1,
            )
            if attempt < len(RETRY_DELAYS) - 1:
                delay = RETRY_DELAYS[attempt]
                if delay > 0:
                    await asyncio.sleep(delay)
    if scan_result is not None:
        status = "CLEAN" if scan_result.clean else "INFECTED"
        result = AVScanResult(
            task_id=request.task_id,
            file_id=file_id,
            status=status,
            engine=scan_result.engine,
            scanned_at=datetime.now(UTC),
            details=AVScanDetails(
                signatures_checked=scan_result.signatures_checked,
                scan_duration_ms=scan_result.scan_duration_ms,
                threats_found=scan_result.threats,
            ),
        )
    else:
        result = AVScanResult(
            task_id=request.task_id,
            file_id=file_id,
            status="ERROR",
            engine="ClamAV",
            scanned_at=datetime.now(UTC),
            details=AVScanDetails(signatures_checked=0, scan_duration_ms=0, threats_found=[]),
        )
    await producer.send(result_topic, str(file_id), result.model_dump_json().encode())
    logger.info("av_scan_result_published", file_id=str(file_id), status=result.status)


async def main() -> None:
    settings = Settings()
    s3 = S3Client(
        endpoint_url=settings.s3_endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )
    scanner = ClamAVScanner(host=settings.clamav_host, port=settings.clamav_port)
    producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_av_request,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="av-worker",
    )
    await consumer.start()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCANS)
    logger.info("av_worker_started")

    async def _process_with_semaphore(msg_value: bytes) -> None:
        async with semaphore:
            try:
                await process_scan_request(
                    raw=msg_value,
                    s3=s3,
                    scanner=scanner,
                    producer=producer,
                    result_topic=settings.kafka_topic_av_result,
                )
            except Exception:
                logger.exception("av_worker_task_failed")

    try:
        async for msg in consumer:
            asyncio.create_task(_process_with_semaphore(msg.value))
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
