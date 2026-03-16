import structlog

from app.domain.kafka_schemas import AVScanResult
from app.domain.quarantine_service import QuarantineService
from app.infrastructure.events import EventProducer
from app.storage.metadata_repository import MetadataRepository

logger = structlog.get_logger()

_TERMINAL_STATUSES = {"CLEAN", "INFECTED"}


class AVResultConsumer:
    def __init__(
        self,
        repo: MetadataRepository,
        quarantine_service: QuarantineService,
        events: EventProducer,
    ) -> None:
        self._repo = repo
        self._quarantine = quarantine_service
        self._events = events

    async def handle_message(self, raw: bytes) -> None:
        result = AVScanResult.model_validate_json(raw)
        file = await self._repo.get_by_id(result.file_id)
        if file is None:
            logger.warning("av_result_file_not_found", file_id=str(result.file_id))
            return
        if file.av_status in _TERMINAL_STATUSES:
            logger.info(
                "av_result_skipped_terminal",
                file_id=str(result.file_id),
                current_status=file.av_status,
            )
            return
        await self._repo.update_av_status(
            file_id=result.file_id,
            av_status=result.status,
            av_scanned_at=result.scanned_at,
            av_engine=result.engine,
            av_report=result.details.model_dump(),
        )
        if result.status == "CLEAN":
            await self._quarantine.promote(file)
            logger.info("av_scan_clean", file_id=str(result.file_id))
        elif result.status == "INFECTED":
            correlation_id = str(file.correlation_id) if file.correlation_id else ""
            await self._events.publish(
                event_type="FILE_AV_FAILED",
                file_id=file.id,
                owner_type=file.owner_type,
                owner_id=str(file.owner_id),
                correlation_id=correlation_id,
                details={"threats": result.details.threats_found},
            )
            logger.warning(
                "av_scan_infected",
                file_id=str(result.file_id),
                threats=result.details.threats_found,
            )
        else:
            logger.error("av_scan_error", file_id=str(result.file_id))
