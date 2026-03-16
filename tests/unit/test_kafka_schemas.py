from datetime import UTC, datetime
from uuid import uuid4

from app.domain.kafka_schemas import AVScanDetails, AVScanRequest, AVScanResult, FileLifecycleEvent


def test_av_scan_request_serializes():
    req = AVScanRequest(
        task_id=uuid4(),
        file_id=uuid4(),
        storage_key="file-id",
        bucket="documents-quarantine",
        content_type="application/pdf",
        size_bytes=1024,
        correlation_id="corr-123",
        requested_at=datetime.now(UTC),
    )
    data = req.model_dump_json()
    assert "file_id" in data and "documents-quarantine" in data


def test_av_scan_result_deserializes():
    result = AVScanResult(
        task_id=uuid4(),
        file_id=uuid4(),
        status="CLEAN",
        engine="ClamAV 1.3.1",
        scanned_at=datetime.now(UTC),
        details=AVScanDetails(
            signatures_checked=8742156,
            scan_duration_ms=340,
            threats_found=[],
        ),
    )
    assert result.status == "CLEAN" and result.details.threats_found == []


def test_lifecycle_event_serializes():
    event = FileLifecycleEvent(
        event_type="FILE_AV_PASSED",
        file_id=uuid4(),
        owner_type="APPLICATION",
        owner_id="owner-123",
        timestamp=datetime.now(UTC),
        correlation_id="corr-123",
        details={"engine": "ClamAV 1.3.1"},
    )
    assert "FILE_AV_PASSED" in event.model_dump_json()
