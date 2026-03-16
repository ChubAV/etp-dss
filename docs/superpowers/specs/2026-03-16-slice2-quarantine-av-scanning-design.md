# Slice 2: Quarantine + AV Scanning — Design Spec

## Overview

Implement the quarantine-based upload flow with antivirus scanning via ClamAV, Kafka-based task orchestration, and file promotion from quarantine to target bucket after successful scan.

## Architecture

### New Components

| Component | Layer | File | Purpose |
|---|---|---|---|
| KafkaProducer | Infrastructure | `app/infrastructure/kafka_producer.py` | Generic aiokafka wrapper for producing messages |
| AVTaskProducer | Infrastructure | `app/infrastructure/av_task_producer.py` | Publish `av.scan.request` to Kafka |
| EventProducer | Infrastructure | `app/infrastructure/events.py` | Publish lifecycle events (`FILE_AV_PASSED`, `FILE_MOVED_TO_STORAGE`, etc.) |
| AVResultConsumer | Infrastructure | `app/infrastructure/av_consumer.py` | Consume `av.scan.result`, orchestrate promotion |
| ScannerProtocol | Domain | `app/domain/scanner.py` | Protocol interface for AV scanners |
| ClamAVScanner | Infrastructure | `app/infrastructure/clamav_scanner.py` | TCP client to ClamAV daemon (INSTREAM) |
| AV Worker | Standalone | `av_worker.py` | Separate process: consume request -> scan -> publish result |
| QuarantineService | Domain | `app/domain/quarantine_service.py` | Copy+verify+delete between S3 buckets |

### Changes to Existing Components

| Component | Change |
|---|---|
| `app/domain/file_service.py` | Upload to quarantine instead of target; publish AV task via AVTaskProducer |
| `app/storage/s3_client.py` | Add `copy_object()`, `delete_object()`, `get_object()` |
| `app/api/v2/routes_download.py` | Check `av_status = CLEAN` before serving |
| `app/api/v2/routes_public.py` | Check `av_status = CLEAN` before serving |
| `app/api/v2/routes_download_token.py` | Check `av_status = CLEAN` before issuing token |
| `app/main.py` | Lifespan: start Kafka producer, AV result consumer |
| `docker-compose.yml` (exists at project root) | Add Kafka (KRaft), ClamAV, AV worker services |
| `app/infrastructure/metrics.py` | Add `dss_av_scan_duration_seconds`, `dss_av_scan_results_total`, `dss_quarantine_files_count` |

Note: The existing `AVNotPassedError` exception in `app/domain/exceptions.py` (already mapped to HTTP 403 in exception handlers) will be reused for the download guard. No new exception needed.

## Data Flow

```
Upload Request
    |
    v
FileService.upload()
    | upload to documents-quarantine (key: {file_id})
    | save metadata (av_status=PENDING, bucket=documents-quarantine, storage_key={file_id})
    | also store target_bucket and target_key in file record for promotion
    | publish av.scan.request -> Kafka
    | set av_status=SCANNING (optimistic: scan request sent)
    v
AV Worker (separate process)
    | consume av.scan.request
    | download file from quarantine via S3 (get_object -> bytes, max 20MB per file)
    | ClamAV INSTREAM scan (TCP, 30s timeout)
    | retry 3x on failure (5/15/30 sec delays)
    | publish av.scan.result -> Kafka
    v
AVResultConsumer (in DSS process)
    | consume av.scan.result
    | idempotency: if av_status already CLEAN/INFECTED, ack and skip
    |-- CLEAN -> QuarantineService.promote()
    |     copy quarantine -> target bucket
    |     verify SHA-256 checksum after copy
    |     delete from quarantine
    |     update bucket, storage_key to target values
    |     update av_status=CLEAN, av_scanned_at, av_engine, av_report
    |     publish FILE_AV_PASSED lifecycle event
    |     publish FILE_MOVED_TO_STORAGE lifecycle event
    |-- INFECTED -> update av_status=INFECTED
    |     publish FILE_AV_FAILED lifecycle event
    |-- ERROR -> update av_status=ERROR
    |     file stays in quarantine for retry
```

## Storage Key Format

- **Quarantine key** (flat): `{file_id}` in `documents-quarantine` bucket
- **Target key** (hierarchical): `{owner_type}/{owner_id}/{file_id}/{version}` in target bucket (public or private)

During upload, both keys are computed. The quarantine key is used for S3 upload; the target key is stored in the `target_key` field of the file record. After promotion, `bucket` and `storage_key` are updated to target values.

### New DB Fields

Add to `files` table:
- `target_bucket: str` — intended final bucket (public or private)
- `target_key: str` — intended final S3 key

After promotion, `bucket` = `target_bucket` and `storage_key` = `target_key`. These fields are nullable (NULL after promotion is complete, or for legacy Slice 1 files).

## Kafka Messages

### AV Scan Request (`documents.av.scan.request`)

```json
{
  "task_id": "uuid",
  "file_id": "uuid",
  "storage_key": "{file_id}",
  "bucket": "documents-quarantine",
  "content_type": "application/pdf",
  "size_bytes": 1048576,
  "correlation_id": "uuid",
  "requested_at": "2025-10-20T12:00:00Z"
}
```

### AV Scan Result (`documents.av.scan.result`)

```json
{
  "task_id": "uuid",
  "file_id": "uuid",
  "status": "CLEAN | INFECTED | ERROR",
  "engine": "ClamAV 1.3.1",
  "scanned_at": "2025-10-20T12:00:15Z",
  "details": {
    "signatures_checked": 8742156,
    "scan_duration_ms": 340,
    "threats_found": []
  }
}
```

### Lifecycle Event (`documents.events`)

```json
{
  "event_type": "FILE_AV_PASSED",
  "file_id": "uuid",
  "owner_type": "APPLICATION",
  "owner_id": "uuid",
  "timestamp": "2025-10-20T12:00:15Z",
  "correlation_id": "uuid",
  "actor_id": null,
  "details": {
    "engine": "ClamAV 1.3.1",
    "scan_duration_ms": 340
  }
}
```

Event types published in this slice: `FILE_AV_PASSED`, `FILE_AV_FAILED`, `FILE_MOVED_TO_STORAGE`.

All Kafka messages use `file_id` as partition key to guarantee ordering per file.

## Scanner Protocol

```python
class ScanResult(BaseModel):
    clean: bool
    engine: str
    threats: list[str]
    signatures_checked: int
    scan_duration_ms: int

class Scanner(Protocol):
    async def scan(self, data: bytes) -> ScanResult: ...
```

The `scan()` method accepts `bytes` intentionally. The AV Worker processes one file at a time with `max_concurrent_scans = 3` (configurable). Given the 20MB file size limit, peak memory for scanning is ~60MB, which is acceptable for a dedicated worker process.

Two implementations:
- **ClamAVScanner** — real TCP INSTREAM scan against ClamAV daemon
- Mock scanner used in unit tests

## Retry Logic

AV Worker retries ClamAV failures locally:
- 3 attempts with delays: 5s, 15s, 30s
- After 3 failures: publish `av.scan.result` with `status=ERROR`
- File remains in quarantine
- Monitoring: metric `dss_av_scan_results_total{status="ERROR"}` + P2 log alert

## Download Guard

All download-related endpoints check `av_status = CLEAN` before proceeding. Uses existing `AVNotPassedError` exception (already mapped to HTTP 403).

| Endpoint | Behavior |
|---|---|
| `POST /download-token` | 403 if `av_status != CLEAN` |
| `GET /{file_id}/download` | 403 if `av_status != CLEAN` |
| `GET /public/{file_id}/download` | 403 if `av_status != CLEAN` |
| `GET /{file_id}/presigned-url` | 403 if `av_status != CLEAN` |
| `GET /{file_id}` (metadata) | Always returns; `av_status` visible in response |

## Status Transitions

```
PENDING -> SCANNING    (DSS sets when publishing av.scan.request)
SCANNING -> CLEAN      (AV passed, no threats)
SCANNING -> INFECTED   (threats found)
SCANNING -> ERROR      (ClamAV unavailable after 3 retries)
ERROR -> PENDING       (manual retry, future feature)
```

Invalid transitions are logged as errors and ignored.

## Idempotency

AVResultConsumer handles duplicate Kafka messages:
- Before processing `av.scan.result`, check current `av_status`
- If already `CLEAN` or `INFECTED`, ack the message and skip
- S3 copy is idempotent; S3 delete on already-deleted object is a no-op
- This ensures at-least-once Kafka delivery does not cause issues

## S3 Client Additions

```python
async def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None
async def delete_object(self, bucket: str, key: str) -> None
async def get_object(self, bucket: str, key: str) -> bytes
```

`get_object` returns full file bytes. This is used only by the AV Worker for files up to 20MB. The main DSS API continues to use streaming/presigned URLs for downloads.

## QuarantineService.promote()

1. S3 COPY from quarantine bucket to target bucket (using `target_bucket` and `target_key` from file record)
2. GET target object metadata, verify SHA-256 matches stored `checksum_sha256`
3. DELETE from quarantine bucket
4. Update file record: `bucket` = `target_bucket`, `storage_key` = `target_key`, `av_status` = `CLEAN`
5. If checksum mismatch: log error, do NOT delete quarantine copy, raise exception

## Docker Compose Additions

Added to existing `docker-compose.yml` at project root:

| Service | Image | Ports | Notes |
|---|---|---|---|
| kafka | `bitnami/kafka:3.7` | 9092 | KRaft mode (no ZooKeeper) |
| clamav | `clamav/clamav:1.3` | 3310 | TCP socket |
| av-worker | Same DSS image | — | `uv run python av_worker.py` |

## Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `dss_av_scan_duration_seconds` | Histogram | `status`, `engine` |
| `dss_av_scan_results_total` | Counter | `status` (CLEAN/INFECTED/ERROR) |
| `dss_quarantine_files_count` | Gauge | — |

## Testing Strategy

| Level | What | Infrastructure |
|---|---|---|
| Unit | QuarantineService, AVTaskProducer, AVResultConsumer, ClamAVScanner, retry logic, download guard, status transitions, idempotency | All mocked |
| Integration | Full cycle: upload -> quarantine -> scan -> promote -> download | docker-compose (Kafka + MinIO + ClamAV + PostgreSQL) |
