import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from app.domain.scanner import ScanResult


def test_av_worker_module_imports():
    from av_worker import process_scan_request

    assert callable(process_scan_request)


def _make_request():
    return json.dumps(
        {
            "task_id": str(uuid4()),
            "file_id": str(uuid4()),
            "storage_key": "file-key",
            "bucket": "documents-quarantine",
            "content_type": "application/pdf",
            "size_bytes": 1024,
            "correlation_id": "corr-123",
            "requested_at": datetime.now(UTC).isoformat(),
        }
    ).encode()


async def test_process_scan_request_clean():
    from av_worker import process_scan_request

    mock_s3 = AsyncMock()
    mock_s3.get_object = AsyncMock(return_value=b"clean-file")
    mock_scanner = AsyncMock()
    mock_scanner.scan = AsyncMock(
        return_value=ScanResult(
            clean=True,
            engine="ClamAV 1.3.1",
            threats=[],
            signatures_checked=100,
            scan_duration_ms=50,
        )
    )
    mock_producer = AsyncMock()
    await process_scan_request(
        raw=_make_request(),
        s3=mock_s3,
        scanner=mock_scanner,
        producer=mock_producer,
        result_topic="documents.av.scan.result",
    )
    mock_s3.get_object.assert_called_once()
    mock_scanner.scan.assert_called_once_with(b"clean-file")
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args
    result_json = json.loads(call_args.args[2])
    assert result_json["status"] == "CLEAN"


async def test_process_scan_request_retry_on_error():
    from av_worker import process_scan_request

    mock_s3 = AsyncMock()
    mock_s3.get_object = AsyncMock(return_value=b"data")
    mock_scanner = AsyncMock()
    mock_scanner.scan = AsyncMock(side_effect=ConnectionRefusedError("ClamAV down"))
    mock_producer = AsyncMock()
    with patch("av_worker.RETRY_DELAYS", [0, 0, 0]):
        await process_scan_request(
            raw=_make_request(),
            s3=mock_s3,
            scanner=mock_scanner,
            producer=mock_producer,
            result_topic="documents.av.scan.result",
        )
    assert mock_scanner.scan.call_count == 3
    call_args = mock_producer.send.call_args
    result_json = json.loads(call_args.args[2])
    assert result_json["status"] == "ERROR"
