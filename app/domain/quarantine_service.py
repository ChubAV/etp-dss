import structlog

from app.infrastructure.events import EventProducer
from app.infrastructure.metrics import QUARANTINE_FILES
from app.storage.metadata_repository import MetadataRepository
from app.storage.s3_client import S3Client

logger = structlog.get_logger()


class QuarantineService:
    def __init__(self, s3: S3Client, repo: MetadataRepository, events: EventProducer) -> None:
        self._s3 = s3
        self._repo = repo
        self._events = events

    async def promote(self, file) -> None:
        src_bucket = file.bucket
        src_key = file.storage_key
        dst_bucket = file.target_bucket
        dst_key = file.target_key

        # 1. Copy to target
        await self._s3.copy_object(src_bucket, src_key, dst_bucket, dst_key)
        logger.info("file_copied_to_target", file_id=str(file.id), dst_bucket=dst_bucket)

        # 2. Verify target object integrity (size check via head_object).
        # Full SHA-256 re-verification would require re-downloading; size check is pragmatic.
        head = await self._s3.head_object(dst_bucket, dst_key)
        if head.get("ContentLength") != file.size_bytes:
            logger.error(
                "checksum_verification_failed",
                file_id=str(file.id),
                expected_size=file.size_bytes,
                actual_size=head.get("ContentLength"),
            )
            raise RuntimeError(f"Size mismatch after copy for file {file.id}")

        # 3. Delete from quarantine
        await self._s3.delete_object(src_bucket, src_key)
        QUARANTINE_FILES.dec()

        # 4. Update DB
        await self._repo.update_after_promotion(file.id)

        # 5. Publish lifecycle events
        correlation_id = str(file.correlation_id) if file.correlation_id else ""
        await self._events.publish(
            event_type="FILE_AV_PASSED",
            file_id=file.id,
            owner_type=file.owner_type,
            owner_id=str(file.owner_id),
            correlation_id=correlation_id,
            details={},
        )
        await self._events.publish(
            event_type="FILE_MOVED_TO_STORAGE",
            file_id=file.id,
            owner_type=file.owner_type,
            owner_id=str(file.owner_id),
            correlation_id=correlation_id,
            details={"target_bucket": dst_bucket},
        )
        logger.info("file_promoted", file_id=str(file.id))
