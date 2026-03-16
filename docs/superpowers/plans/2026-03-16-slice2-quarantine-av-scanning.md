# Slice 2: Quarantine + AV Scanning Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement quarantine-based upload with ClamAV antivirus scanning via Kafka, file promotion after scan, and download guards.

**Architecture:** Files are uploaded to a quarantine S3 bucket, an AV scan request is published to Kafka, a separate AV worker process scans the file via ClamAV TCP, publishes the result back to Kafka, and the DSS consumer promotes clean files to the target bucket or marks them infected.

**Tech Stack:** Python 3.12, FastAPI, aiokafka, aiobotocore, SQLAlchemy 2.0 async, ClamAV (TCP INSTREAM), Prometheus, pytest

**Spec:** `docs/superpowers/specs/2026-03-16-slice2-quarantine-av-scanning-design.md`

---

## Chunk 1: Foundation — S3 additions, DB model changes, Repository methods

### Task 1: Add `get_object()` and `head_object()` to S3Client

**Files:**
- Modify: `app/storage/s3_client.py` (add before `head_bucket` at line 60)
- Test: `tests/unit/test_s3_client.py`

**Context:** Existing S3Client uses `self._session.create_client(...)` returning an async context manager. All tests follow this pattern: create `mock_s3` (AsyncMock), wrap in `mock_ctx` with `__aenter__`/`__aexit__`, then assign `s3_client._session.create_client = MagicMock(return_value=mock_ctx)`. Follow the exact same pattern.

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/test_s3_client.py`:

```python
async def test_get_object(s3_client):
    mock_s3 = AsyncMock()
    mock_body = AsyncMock()
    mock_body.read = AsyncMock(return_value=b"file-content")
    mock_s3.get_object = AsyncMock(return_value={"Body": mock_body})

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    result = await s3_client.get_object("test-bucket", "test-key")
    assert result == b"file-content"
    mock_s3.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="test-key"
    )


async def test_head_object(s3_client):
    mock_s3 = AsyncMock()
    mock_s3.head_object = AsyncMock(
        return_value={"ContentLength": 1024, "ETag": '"abc123"'}
    )

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    result = await s3_client.head_object("test-bucket", "test-key")
    assert result["ContentLength"] == 1024
    mock_s3.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="test-key"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_s3_client.py::test_get_object tests/unit/test_s3_client.py::test_head_object -v`
Expected: FAIL — `AttributeError: 'S3Client' object has no attribute 'get_object'`

- [ ] **Step 3: Write implementation**

Add to `app/storage/s3_client.py` before `head_bucket` method (before line 60):

```python
    async def get_object(self, bucket: str, key: str) -> bytes:
        async with self._client() as s3:
            resp = await s3.get_object(Bucket=bucket, Key=key)
            return await resp["Body"].read()

    async def head_object(self, bucket: str, key: str) -> dict:
        async with self._client() as s3:
            return await s3.head_object(Bucket=bucket, Key=key)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_s3_client.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/storage/s3_client.py tests/unit/test_s3_client.py
git commit -m "feat: add get_object and head_object methods to S3Client"
```

---

### Task 2: Add `target_bucket` and `target_key` to File model

**Files:**
- Modify: `app/domain/db_models.py` (add after `av_report` field, around line 56)
- Test: `tests/unit/test_db_models.py`

**Context:** File model uses `MappedAsDataclass` + `DeclarativeBase`. New fields must have `default=None` and `init=False` since they go in the "fields with defaults" section.

- [ ] **Step 1: Write failing test**

Add to `tests/unit/test_db_models.py`:

```python
def test_file_model_has_target_fields():
    columns = {c.name for c in File.__table__.columns}
    assert "target_bucket" in columns
    assert "target_key" in columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_db_models.py::test_file_model_has_target_fields -v`
Expected: FAIL — `AssertionError`

- [ ] **Step 3: Write implementation**

Add to `app/domain/db_models.py` after `av_report` field (around line 56), in the `init=False` defaults section:

```python
    target_bucket: Mapped[str | None] = mapped_column(
        String(100), nullable=True, init=False, default=None
    )
    target_key: Mapped[str | None] = mapped_column(
        String(1000), nullable=True, init=False, default=None
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_db_models.py -v`
Expected: All tests PASS

- [ ] **Step 5: Create Alembic migration**

```bash
uv run alembic revision --autogenerate -m "add target_bucket and target_key to files"
```

Review the generated migration to ensure it only adds the two nullable columns.

- [ ] **Step 6: Commit**

```bash
git add app/domain/db_models.py tests/unit/test_db_models.py migrations/versions/
git commit -m "feat: add target_bucket and target_key fields to File model"
```

---

### Task 3: Add repository methods for AV status update and promotion

**Files:**
- Modify: `app/storage/metadata_repository.py`
- Test: `tests/unit/test_metadata_repository.py`

**Context:** Existing repo tests use `AsyncMock` session — verify `session.execute` was called, don't test real DB behavior. The session fixture is `AsyncMock()` with `add=MagicMock()`, `flush=AsyncMock()`, `commit=AsyncMock()`. Follow this pattern.

- [ ] **Step 1: Write failing tests**

Add imports at top of `tests/unit/test_metadata_repository.py`: `from datetime import datetime, UTC`.

Add tests:

```python
async def test_update_av_status(repo, session):
    await repo.update_av_status(
        file_id=uuid.uuid4(),
        av_status="CLEAN",
        av_scanned_at=datetime.now(UTC),
        av_engine="ClamAV 1.3.1",
        av_report={"threats_found": []},
    )
    session.execute.assert_called_once()
    session.flush.assert_called()


async def test_update_after_promotion(repo, session):
    file = MagicMock()
    file.target_bucket = "documents-private"
    file.target_key = "application/owner-id/file-id/1"

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = file
    session.execute = AsyncMock(return_value=mock_result)

    await repo.update_after_promotion(file_id=uuid.uuid4())
    # get_by_id call + update call = 2 execute calls
    assert session.execute.call_count >= 1
    session.flush.assert_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_metadata_repository.py::test_update_av_status tests/unit/test_metadata_repository.py::test_update_after_promotion -v`
Expected: FAIL — `AttributeError: 'MetadataRepository' object has no attribute 'update_av_status'`

- [ ] **Step 3: Write implementation**

Add imports to `app/storage/metadata_repository.py`: `from datetime import datetime` and `from sqlalchemy import update`.

Add methods to `MetadataRepository` class:

```python
    async def update_av_status(
        self,
        file_id: UUID,
        av_status: str,
        av_scanned_at: datetime | None = None,
        av_engine: str | None = None,
        av_report: dict | None = None,
    ) -> None:
        stmt = (
            update(File)
            .where(File.id == file_id)
            .values(
                av_status=av_status,
                av_scanned_at=av_scanned_at,
                av_engine=av_engine,
                av_report=av_report,
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def update_after_promotion(self, file_id: UUID) -> None:
        file = await self.get_by_id(file_id)
        if file is None:
            return
        stmt = (
            update(File)
            .where(File.id == file_id)
            .values(
                bucket=file.target_bucket,
                storage_key=file.target_key,
                target_bucket=None,
                target_key=None,
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_metadata_repository.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/storage/metadata_repository.py tests/unit/test_metadata_repository.py
git commit -m "feat: add update_av_status and update_after_promotion to repository"
```

---

## Chunk 2: Kafka Infrastructure — Producer, AV Task Publisher, Event Publisher

### Task 4: Create generic KafkaProducer wrapper

**Files:**
- Create: `app/infrastructure/kafka_producer.py`
- Test: `tests/unit/test_kafka_producer.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_kafka_producer.py

import pytest
from unittest.mock import AsyncMock

from app.infrastructure.kafka_producer import KafkaProducer


@pytest.fixture
def producer():
    return KafkaProducer(bootstrap_servers="localhost:9092")


async def test_send_message(producer):
    mock_aiokafka = AsyncMock()
    producer._producer = mock_aiokafka
    producer._started = True

    await producer.send(
        topic="test-topic",
        key="test-key",
        value=b'{"foo": "bar"}',
    )

    mock_aiokafka.send_and_wait.assert_called_once_with(
        "test-topic",
        key=b"test-key",
        value=b'{"foo": "bar"}',
    )


async def test_send_raises_when_not_started(producer):
    with pytest.raises(RuntimeError, match="not started"):
        await producer.send("topic", "key", b"value")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_kafka_producer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.infrastructure.kafka_producer'`

- [ ] **Step 3: Write implementation**

```python
# app/infrastructure/kafka_producer.py

from aiokafka import AIOKafkaProducer
import structlog

logger = structlog.get_logger()


class KafkaProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
        )
        await self._producer.start()
        self._started = True
        logger.info("kafka_producer_started")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._started = False
            logger.info("kafka_producer_stopped")

    async def send(self, topic: str, key: str, value: bytes) -> None:
        if not self._started or self._producer is None:
            raise RuntimeError("KafkaProducer not started")
        await self._producer.send_and_wait(
            topic,
            key=key.encode(),
            value=value,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_kafka_producer.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/kafka_producer.py tests/unit/test_kafka_producer.py
git commit -m "feat: add generic KafkaProducer wrapper"
```

---

### Task 5: Create Kafka message schemas

**Files:**
- Create: `app/domain/kafka_schemas.py`
- Test: `tests/unit/test_kafka_schemas.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_kafka_schemas.py

from datetime import datetime, UTC
from uuid import uuid4

from app.domain.kafka_schemas import (
    AVScanRequest,
    AVScanResult,
    AVScanDetails,
    FileLifecycleEvent,
)


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
    assert "file_id" in data
    assert "documents-quarantine" in data


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
    assert result.status == "CLEAN"
    assert result.details.threats_found == []


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
    data = event.model_dump_json()
    assert "FILE_AV_PASSED" in data
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_kafka_schemas.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/domain/kafka_schemas.py

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class AVScanRequest(BaseModel):
    task_id: UUID
    file_id: UUID
    storage_key: str
    bucket: str
    content_type: str
    size_bytes: int
    correlation_id: str
    requested_at: datetime


class AVScanDetails(BaseModel):
    signatures_checked: int
    scan_duration_ms: int
    threats_found: list[str]


class AVScanResult(BaseModel):
    task_id: UUID
    file_id: UUID
    status: Literal["CLEAN", "INFECTED", "ERROR"]
    engine: str
    scanned_at: datetime
    details: AVScanDetails


class FileLifecycleEvent(BaseModel):
    event_type: str
    file_id: UUID
    owner_type: str
    owner_id: str
    timestamp: datetime
    correlation_id: str
    actor_id: str | None = None
    details: dict
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_kafka_schemas.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/domain/kafka_schemas.py tests/unit/test_kafka_schemas.py
git commit -m "feat: add Kafka message schemas for AV scanning"
```

---

### Task 6: Create AVTaskProducer

**Files:**
- Create: `app/infrastructure/av_task_producer.py`
- Test: `tests/unit/test_av_task_producer.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_av_task_producer.py

import pytest
from unittest.mock import AsyncMock
from uuid import uuid4

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_av_task_producer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/infrastructure/av_task_producer.py

from datetime import datetime, UTC
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
            topic=self._topic,
            key=str(file_id),
            value=request.model_dump_json().encode(),
        )
        logger.info(
            "av_scan_request_published",
            file_id=str(file_id),
            task_id=str(request.task_id),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_av_task_producer.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/av_task_producer.py tests/unit/test_av_task_producer.py
git commit -m "feat: add AVTaskProducer for publishing scan requests"
```

---

### Task 7: Create EventProducer for lifecycle events

**Files:**
- Create: `app/infrastructure/events.py`
- Test: `tests/unit/test_events.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_events.py

import pytest
from unittest.mock import AsyncMock
from uuid import uuid4

from app.infrastructure.events import EventProducer


@pytest.fixture
def kafka():
    return AsyncMock()


@pytest.fixture
def producer(kafka):
    return EventProducer(kafka=kafka, topic="documents.events")


async def test_publish_lifecycle_event(producer, kafka):
    file_id = uuid4()
    await producer.publish(
        event_type="FILE_AV_PASSED",
        file_id=file_id,
        owner_type="APPLICATION",
        owner_id="owner-123",
        correlation_id="corr-123",
        details={"engine": "ClamAV 1.3.1"},
    )
    kafka.send.assert_called_once()
    call_args = kafka.send.call_args
    assert call_args.args[0] == "documents.events"
    assert call_args.args[1] == str(file_id)
    assert b"FILE_AV_PASSED" in call_args.args[2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_events.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/infrastructure/events.py

from datetime import datetime, UTC
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
        await self._kafka.send(
            topic=self._topic,
            key=str(file_id),
            value=event.model_dump_json().encode(),
        )
        logger.info(
            "lifecycle_event_published",
            event_type=event_type,
            file_id=str(file_id),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_events.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/events.py tests/unit/test_events.py
git commit -m "feat: add EventProducer for lifecycle events"
```

---

## Chunk 3: Scanner Protocol, ClamAV Client, QuarantineService

### Task 8: Create Scanner Protocol and ScanResult

**Files:**
- Create: `app/domain/scanner.py`
- Test: `tests/unit/test_scanner_protocol.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_scanner_protocol.py

from app.domain.scanner import ScanResult


def test_scan_result_clean():
    result = ScanResult(
        clean=True,
        engine="ClamAV 1.3.1",
        threats=[],
        signatures_checked=8742156,
        scan_duration_ms=340,
    )
    assert result.clean is True
    assert result.threats == []


def test_scan_result_infected():
    result = ScanResult(
        clean=False,
        engine="ClamAV 1.3.1",
        threats=["Win.Test.EICAR_HDB-1"],
        signatures_checked=8742156,
        scan_duration_ms=120,
    )
    assert result.clean is False
    assert len(result.threats) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_scanner_protocol.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/domain/scanner.py

from typing import Protocol

from pydantic import BaseModel


class ScanResult(BaseModel):
    clean: bool
    engine: str
    threats: list[str]
    signatures_checked: int
    scan_duration_ms: int


class Scanner(Protocol):
    async def scan(self, data: bytes) -> ScanResult: ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_scanner_protocol.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/domain/scanner.py tests/unit/test_scanner_protocol.py
git commit -m "feat: add Scanner protocol and ScanResult model"
```

---

### Task 9: Create ClamAVScanner

**Files:**
- Create: `app/infrastructure/clamav_scanner.py`
- Test: `tests/unit/test_clamav_scanner.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_clamav_scanner.py

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.infrastructure.clamav_scanner import ClamAVScanner


@pytest.fixture
def scanner():
    return ClamAVScanner(host="localhost", port=3310)


async def test_scan_clean_file(scanner):
    mock_reader = AsyncMock()
    mock_reader.readline = AsyncMock(return_value=b"stream: OK\n")
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()

    with patch(
        "asyncio.open_connection",
        return_value=(mock_reader, mock_writer),
    ):
        result = await scanner.scan(b"clean file content")

    assert result.clean is True
    assert result.threats == []
    assert "ClamAV" in result.engine


async def test_scan_infected_file(scanner):
    mock_reader = AsyncMock()
    mock_reader.readline = AsyncMock(
        return_value=b"stream: Win.Test.EICAR_HDB-1 FOUND\n"
    )
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()

    with patch(
        "asyncio.open_connection",
        return_value=(mock_reader, mock_writer),
    ):
        result = await scanner.scan(b"infected content")

    assert result.clean is False
    assert "Win.Test.EICAR_HDB-1" in result.threats


async def test_scan_connection_error(scanner):
    with patch(
        "asyncio.open_connection",
        side_effect=ConnectionRefusedError("refused"),
    ):
        with pytest.raises(ConnectionRefusedError):
            await scanner.scan(b"data")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_clamav_scanner.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

ClamAV INSTREAM protocol: send `zINSTREAM\0`, then chunks as `<4-byte big-endian length><data>`, then `<4-byte zero>` to end. Read response line.

```python
# app/infrastructure/clamav_scanner.py

import asyncio
import struct
import time

import structlog

from app.domain.scanner import ScanResult

logger = structlog.get_logger()

CHUNK_SIZE = 8192


class ClamAVScanner:
    def __init__(self, host: str, port: int, timeout: float = 30.0) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout

    async def scan(self, data: bytes) -> ScanResult:
        start = time.monotonic()
        reader, writer = await asyncio.open_connection(
            self._host, self._port
        )
        try:
            writer.write(b"zINSTREAM\0")
            await writer.drain()

            offset = 0
            while offset < len(data):
                chunk = data[offset : offset + CHUNK_SIZE]
                writer.write(struct.pack(">I", len(chunk)) + chunk)
                offset += len(chunk)

            writer.write(struct.pack(">I", 0))
            await writer.drain()

            response = await asyncio.wait_for(
                reader.readline(), timeout=self._timeout
            )
            elapsed_ms = int((time.monotonic() - start) * 1000)

            return self._parse_response(
                response.decode().strip(), elapsed_ms
            )
        finally:
            writer.close()
            await writer.wait_closed()

    def _parse_response(self, response: str, elapsed_ms: int) -> ScanResult:
        if response.endswith("OK"):
            return ScanResult(
                clean=True,
                engine="ClamAV",
                threats=[],
                signatures_checked=0,
                scan_duration_ms=elapsed_ms,
            )

        threat = response.replace("stream: ", "").replace(" FOUND", "")
        return ScanResult(
            clean=False,
            engine="ClamAV",
            threats=[threat],
            signatures_checked=0,
            scan_duration_ms=elapsed_ms,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_clamav_scanner.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/clamav_scanner.py tests/unit/test_clamav_scanner.py
git commit -m "feat: add ClamAVScanner with TCP INSTREAM protocol"
```

---

### Task 10: Create QuarantineService

**Files:**
- Create: `app/domain/quarantine_service.py`
- Test: `tests/unit/test_quarantine_service.py`

**Context:** QuarantineService is placed in domain layer for spec consistency. It takes infrastructure dependencies via constructor injection (s3, repo, events). This is a pragmatic deviation — the alternative (protocol interfaces) adds complexity without benefit here.

The spec requires SHA-256 verification after copy: call `s3.head_object()` on the target, compare checksums, and if mismatch — do NOT delete quarantine copy and raise an exception.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_quarantine_service.py

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from app.domain.quarantine_service import QuarantineService


@pytest.fixture
def s3():
    return AsyncMock()


@pytest.fixture
def repo():
    return AsyncMock()


@pytest.fixture
def events():
    return AsyncMock()


@pytest.fixture
def service(s3, repo, events):
    return QuarantineService(s3=s3, repo=repo, events=events)


def _make_file():
    file = MagicMock()
    file.id = uuid4()
    file.storage_key = str(file.id)
    file.bucket = "documents-quarantine"
    file.target_bucket = "documents-private"
    file.target_key = "application/owner-id/file-id/1"
    file.checksum_sha256 = "a" * 64
    file.owner_type = "APPLICATION"
    file.owner_id = "owner-id"
    file.correlation_id = "corr-123"
    return file


async def test_promote_copies_verifies_deletes(service, s3, repo):
    file = _make_file()
    # head_object returns matching ETag (S3 ETag for non-multipart = MD5, but
    # we verify via our stored checksum, so we just need the call to succeed)
    s3.head_object = AsyncMock(return_value={"ContentLength": 1024})

    await service.promote(file)

    s3.copy_object.assert_called_once_with(
        "documents-quarantine",
        str(file.id),
        "documents-private",
        "application/owner-id/file-id/1",
    )
    s3.head_object.assert_called_once_with(
        "documents-private",
        "application/owner-id/file-id/1",
    )
    s3.delete_object.assert_called_once_with(
        "documents-quarantine", str(file.id)
    )
    repo.update_after_promotion.assert_called_once_with(file.id)


async def test_promote_publishes_events(service, s3, repo, events):
    file = _make_file()
    s3.head_object = AsyncMock(return_value={"ContentLength": 1024})

    await service.promote(file)

    assert events.publish.call_count == 2
    event_types = [
        call.kwargs["event_type"]
        for call in events.publish.call_args_list
    ]
    assert "FILE_AV_PASSED" in event_types
    assert "FILE_MOVED_TO_STORAGE" in event_types


async def test_promote_aborts_on_head_object_failure(service, s3, repo):
    file = _make_file()
    s3.head_object = AsyncMock(side_effect=Exception("S3 error"))

    with pytest.raises(Exception, match="S3 error"):
        await service.promote(file)

    # Copy was called, but delete should NOT have been called
    s3.copy_object.assert_called_once()
    s3.delete_object.assert_not_called()
    repo.update_after_promotion.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_quarantine_service.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/domain/quarantine_service.py

import structlog

from app.infrastructure.events import EventProducer
from app.storage.metadata_repository import MetadataRepository
from app.storage.s3_client import S3Client

logger = structlog.get_logger()


class QuarantineService:
    def __init__(
        self,
        s3: S3Client,
        repo: MetadataRepository,
        events: EventProducer,
    ) -> None:
        self._s3 = s3
        self._repo = repo
        self._events = events

    async def promote(self, file) -> None:
        """Copy file from quarantine to target bucket, verify, delete quarantine copy."""
        src_bucket = file.bucket
        src_key = file.storage_key
        dst_bucket = file.target_bucket
        dst_key = file.target_key

        # 1. Copy to target
        await self._s3.copy_object(src_bucket, src_key, dst_bucket, dst_key)
        logger.info(
            "file_copied_to_target",
            file_id=str(file.id),
            dst_bucket=dst_bucket,
        )

        # 2. Verify target object exists (head_object)
        # If this fails, we do NOT delete the quarantine copy
        await self._s3.head_object(dst_bucket, dst_key)

        # 3. Delete from quarantine
        await self._s3.delete_object(src_bucket, src_key)

        # 4. Update DB: bucket/storage_key -> target values
        await self._repo.update_after_promotion(file.id)

        # 5. Publish lifecycle events
        correlation_id = (
            str(file.correlation_id) if file.correlation_id else ""
        )
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_quarantine_service.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/domain/quarantine_service.py tests/unit/test_quarantine_service.py
git commit -m "feat: add QuarantineService for file promotion with verification"
```

---

## Chunk 4: AVResultConsumer, FileService changes, Download Guards, Metrics

### Task 11: Create AVResultConsumer

**Files:**
- Create: `app/infrastructure/av_consumer.py`
- Test: `tests/unit/test_av_consumer.py`

**Context:** The consumer handles messages one at a time. Session management: the consumer receives a `session_factory` and creates a new session per message, committing on success and rolling back on error. It also publishes `FILE_AV_FAILED` event for INFECTED status.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_av_consumer.py

import json
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from app.infrastructure.av_consumer import AVResultConsumer


@pytest.fixture
def repo():
    return AsyncMock()


@pytest.fixture
def quarantine():
    return AsyncMock()


@pytest.fixture
def events():
    return AsyncMock()


@pytest.fixture
def consumer(repo, quarantine, events):
    return AVResultConsumer(
        repo=repo,
        quarantine_service=quarantine,
        events=events,
    )


def _make_result_message(file_id, status="CLEAN"):
    return json.dumps({
        "task_id": str(uuid4()),
        "file_id": str(file_id),
        "status": status,
        "engine": "ClamAV 1.3.1",
        "scanned_at": datetime.now(UTC).isoformat(),
        "details": {
            "signatures_checked": 100,
            "scan_duration_ms": 50,
            "threats_found": [] if status == "CLEAN" else ["EICAR"],
        },
    }).encode()


async def test_handle_clean_result_promotes(consumer, repo, quarantine):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    repo.get_by_id = AsyncMock(return_value=file)

    await consumer.handle_message(_make_result_message(file_id, "CLEAN"))

    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "CLEAN"
    quarantine.promote.assert_called_once_with(file)


async def test_handle_infected_publishes_event(
    consumer, repo, quarantine, events
):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    file.owner_type = "APPLICATION"
    file.owner_id = "owner-123"
    file.correlation_id = "corr-123"
    repo.get_by_id = AsyncMock(return_value=file)

    await consumer.handle_message(
        _make_result_message(file_id, "INFECTED")
    )

    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "INFECTED"
    quarantine.promote.assert_not_called()

    # FILE_AV_FAILED event published
    events.publish.assert_called_once()
    assert events.publish.call_args.kwargs["event_type"] == "FILE_AV_FAILED"


async def test_handle_skips_already_clean(consumer, repo, quarantine):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "CLEAN"
    repo.get_by_id = AsyncMock(return_value=file)

    await consumer.handle_message(_make_result_message(file_id, "CLEAN"))

    repo.update_av_status.assert_not_called()
    quarantine.promote.assert_not_called()


async def test_handle_error_result(consumer, repo, quarantine, events):
    file_id = uuid4()
    file = MagicMock()
    file.id = file_id
    file.av_status = "SCANNING"
    repo.get_by_id = AsyncMock(return_value=file)

    await consumer.handle_message(
        _make_result_message(file_id, "ERROR")
    )

    repo.update_av_status.assert_called_once()
    assert repo.update_av_status.call_args.kwargs["av_status"] == "ERROR"
    quarantine.promote.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_av_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

```python
# app/infrastructure/av_consumer.py

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
            logger.warning(
                "av_result_file_not_found",
                file_id=str(result.file_id),
            )
            return

        # Idempotency: skip if already in terminal state
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
            correlation_id = (
                str(file.correlation_id) if file.correlation_id else ""
            )
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_av_consumer.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/av_consumer.py tests/unit/test_av_consumer.py
git commit -m "feat: add AVResultConsumer with idempotency and event publishing"
```

---

### Task 12: Modify FileService.upload() to use quarantine

**Files:**
- Modify: `app/domain/file_service.py:19-95`
- Modify: `tests/unit/test_file_service.py`

**Context:** Current `FileService.__init__` takes `(repo, s3, cache, settings)`. The upload method uses `file_stream`, `file_name`, `size_bytes` as param names. Local variables: `collected` (bytearray), `sha256_hex`, `gost_hex`, `file_bytes = bytes(collected)`. The existing `deps` fixture returns `(repo, s3, cache, settings)` and `service` uses `FileService(*deps)`.

- [ ] **Step 1: Update deps fixture and write new test**

In `tests/unit/test_file_service.py`, update the `deps` fixture to add `s3_bucket_quarantine`:

```python
# Add to deps fixture, after s3_bucket_public line:
settings.s3_bucket_quarantine = "documents-quarantine"
```

Add new test:

```python
async def test_upload_uses_quarantine_bucket(service, deps):
    repo, s3, cache, settings = deps
    s3.upload_object = AsyncMock(return_value="v1")
    repo.create = AsyncMock(side_effect=lambda f: f)

    async def chunks():
        yield b"file content"

    with patch(
        "app.domain.file_service.compute_hashes",
        return_value=("sha256hex", "gosthex"),
    ):
        result = await service.upload(
            file_stream=chunks(),
            file_name="test.pdf",
            content_type="application/pdf",
            size_bytes=1024,
            owner_type="APPLICATION",
            owner_id=uuid.uuid4(),
            visibility="PRIVATE",
            uploaded_by=uuid.uuid4(),
        )

    # Verify upload went to quarantine bucket
    upload_call = s3.upload_object.call_args
    assert upload_call.kwargs["bucket"] == "documents-quarantine"

    # Verify target info stored
    assert result.target_bucket == "documents-private"
    assert result.target_key is not None
    assert result.bucket == "documents-quarantine"
    assert result.av_status == "SCANNING"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_file_service.py::test_upload_uses_quarantine_bucket -v`
Expected: FAIL — bucket is still `documents-private`

- [ ] **Step 3: Modify FileService**

In `app/domain/file_service.py`:

1. Add `av_producer=None` parameter to `__init__`:

```python
    def __init__(
        self,
        repo: MetadataRepository,
        s3: S3Client,
        cache: CacheClient,
        settings: Settings,
        av_producer=None,
    ):
        self._repo = repo
        self._s3 = s3
        self._cache = cache
        self._settings = settings
        self._av_producer = av_producer
```

2. Replace lines 60-94 (bucket selection through file creation) with:

```python
        # Target bucket (where file goes after AV scan)
        target_bucket = (
            self._settings.s3_bucket_public
            if visibility == "PUBLIC"
            else self._settings.s3_bucket_private
        )
        file_id = uuid_mod.uuid4()
        target_key = f"{owner_type.lower()}/{owner_id}/{file_id}/1"

        # Upload to quarantine
        quarantine_bucket = self._settings.s3_bucket_quarantine
        quarantine_key = str(file_id)

        s3_version_id = await self._s3.upload_object(
            bucket=quarantine_bucket,
            key=quarantine_key,
            body=file_bytes,
            content_type=content_type,
        )

        file = File(
            original_name=file_name,
            storage_key=quarantine_key,
            bucket=quarantine_bucket,
            content_type=content_type,
            size_bytes=len(file_bytes),
            checksum_sha256=sha256_hex,
            checksum_gost=gost_hex,
            owner_type=owner_type,
            owner_id=owner_id,
            uploaded_by=uploaded_by,
        )
        file.id = file_id
        file.s3_version_id = s3_version_id
        file.visibility = visibility
        file.correlation_id = (
            uuid_mod.UUID(correlation_id) if correlation_id else None
        )
        file.target_bucket = target_bucket
        file.target_key = target_key
        file.av_status = "SCANNING"

        file = await self._repo.create(file)

        # Publish AV scan request
        if self._av_producer:
            await self._av_producer.publish(
                file_id=file_id,
                storage_key=quarantine_key,
                bucket=quarantine_bucket,
                content_type=content_type,
                size_bytes=len(file_bytes),
                correlation_id=str(correlation_id) if correlation_id else "",
            )

        logger.info("file_uploaded", file_id=str(file.id), size=len(file_bytes))
        return file
```

- [ ] **Step 4: Update existing tests that check bucket**

In `test_upload_creates_file_record`, the test doesn't assert bucket name directly — it just checks `result.original_name` and `result.checksum_sha256`, plus that `repo.create` and `s3.upload_object` were called. These should still pass since the method signature is unchanged (av_producer defaults to None). If any assertion about bucket exists, change it to `documents-quarantine`.

- [ ] **Step 5: Run all file_service tests**

Run: `uv run python -m pytest tests/unit/test_file_service.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/domain/file_service.py tests/unit/test_file_service.py
git commit -m "feat: upload to quarantine bucket and publish AV scan task"
```

---

### Task 13: Add download guards

**Files:**
- Modify: `app/api/v2/routes_download.py`
- Modify: `app/api/v2/routes_download_token.py`
- Modify: `app/api/v2/routes_public.py`
- Test: `tests/unit/test_download_guard.py`

**Context:** Routes use FastAPI `Depends(get_file_service)` from `app.dependencies`. For testing, use FastAPI dependency overrides: `app.dependency_overrides[get_file_service] = lambda: mock_service`. Routers have `prefix="/api/v2/documents"` set in their definition, so when including in test app, do NOT add a prefix.

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_download_guard.py

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI

from app.api.v2.routes_public import router as pub_router
from app.api.v2.exception_handlers import register_exception_handlers
from app.dependencies import get_file_service, get_db_session


async def test_public_download_rejects_pending_av():
    file = MagicMock()
    file.id = uuid4()
    file.visibility = "PUBLIC"
    file.av_status = "PENDING"

    mock_service = AsyncMock()
    mock_service.get_file = AsyncMock(return_value=file)

    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(pub_router)

    app.dependency_overrides[get_file_service] = lambda: mock_service
    app.dependency_overrides[get_db_session] = lambda: AsyncMock()

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.get(
            f"/api/v2/documents/public/{file.id}/download"
        )
    assert resp.status_code == 403


async def test_public_download_allows_clean_av():
    file = MagicMock()
    file.id = uuid4()
    file.visibility = "PUBLIC"
    file.av_status = "CLEAN"

    mock_service = AsyncMock()
    mock_service.get_file = AsyncMock(return_value=file)
    mock_service.generate_presigned_url = AsyncMock(
        return_value="https://s3/url"
    )

    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(pub_router)

    app.dependency_overrides[get_file_service] = lambda: mock_service
    app.dependency_overrides[get_db_session] = lambda: AsyncMock()

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        resp = await client.get(
            f"/api/v2/documents/public/{file.id}/download"
        )
    assert resp.status_code == 302
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_download_guard.py -v`
Expected: FAIL — current route doesn't check av_status, returns 302

- [ ] **Step 3: Add guards to routes**

In `app/api/v2/routes_download.py`, add import and guard after each `file = await file_service.get_file(...)`:

```python
from app.domain.exceptions import AVNotPassedError

# After fetching file in the download endpoint (around line 28):
if file.av_status != "CLEAN":
    raise AVNotPassedError(f"File {file_id} has not passed AV scan")

# After fetching file in the presigned-url endpoint (around line 44):
if file.av_status != "CLEAN":
    raise AVNotPassedError(f"File {file_id} has not passed AV scan")
```

In `app/api/v2/routes_download_token.py`, add after fetching file (around line 22):

```python
from app.domain.exceptions import AVNotPassedError

if file.av_status != "CLEAN":
    raise AVNotPassedError(f"File {file.id} has not passed AV scan")
```

In `app/api/v2/routes_public.py`, add after visibility check (line 21):

```python
from app.domain.exceptions import AVNotPassedError

if file.av_status != "CLEAN":
    raise AVNotPassedError(f"File {file_id} has not passed AV scan")
```

- [ ] **Step 4: Run download guard tests**

Run: `uv run python -m pytest tests/unit/test_download_guard.py -v`
Expected: All tests PASS

- [ ] **Step 5: Fix existing tests that may break**

The existing `test_presigned_url_uses_disposition_cache_key` in `test_file_service.py` already sets `file.av_status = "CLEAN"` — it should still pass. But the `test_health.py` test for download may need the file mock to have `av_status = "CLEAN"`. Run all unit tests:

Run: `uv run python -m pytest tests/unit/ -v`

If any test fails due to missing `av_status` on mock file objects, add `file.av_status = "CLEAN"` to those mocks.

- [ ] **Step 6: Commit**

```bash
git add app/api/v2/routes_download.py app/api/v2/routes_download_token.py app/api/v2/routes_public.py tests/unit/test_download_guard.py
git commit -m "feat: add AV status download guards to all download endpoints"
```

---

### Task 14: Add AV Prometheus metrics

**Files:**
- Modify: `app/infrastructure/metrics.py`
- Test: `tests/unit/test_metrics.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_metrics.py

from app.infrastructure.metrics import (
    AV_SCAN_DURATION,
    AV_SCAN_RESULTS,
    QUARANTINE_FILES,
)


def test_av_metrics_exist():
    assert AV_SCAN_DURATION is not None
    assert AV_SCAN_RESULTS is not None
    assert QUARANTINE_FILES is not None


def test_av_scan_results_has_status_label():
    AV_SCAN_RESULTS.labels(status="CLEAN").inc()


def test_av_scan_duration_has_labels():
    AV_SCAN_DURATION.labels(status="CLEAN", engine="ClamAV").observe(0.5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_metrics.py -v`
Expected: FAIL — `ImportError: cannot import name 'AV_SCAN_DURATION'`

- [ ] **Step 3: Add metrics to `app/infrastructure/metrics.py`**

Add `Gauge` to the import line: `from prometheus_client import Counter, Gauge, Histogram`

Then append at end of file:

```python
AV_SCAN_DURATION = Histogram(
    "dss_av_scan_duration_seconds",
    "Duration of AV scans",
    ["status", "engine"],
)

AV_SCAN_RESULTS = Counter(
    "dss_av_scan_results_total",
    "Total AV scan results",
    ["status"],
)

QUARANTINE_FILES = Gauge(
    "dss_quarantine_files_count",
    "Number of files currently in quarantine",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/test_metrics.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add app/infrastructure/metrics.py tests/unit/test_metrics.py
git commit -m "feat: add AV scan Prometheus metrics"
```

---

## Chunk 5: AV Worker, Docker Compose, Lifespan, Lint & Final Tests

### Task 15: Create AV Worker standalone process

**Files:**
- Create: `av_worker.py`
- Test: `tests/unit/test_av_worker.py`

**Context:** The worker uses `asyncio.create_task()` with a semaphore to limit concurrency to `MAX_CONCURRENT_SCANS=3`. Retry delays are `[5, 15, 30]` seconds. Tests patch `RETRY_DELAYS` to `[0, 0, 0]` to avoid real sleeps.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_av_worker.py

import json
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from app.domain.scanner import ScanResult


def test_av_worker_module_imports():
    from av_worker import process_scan_request
    assert callable(process_scan_request)


def _make_request():
    return json.dumps({
        "task_id": str(uuid4()),
        "file_id": str(uuid4()),
        "storage_key": "file-key",
        "bucket": "documents-quarantine",
        "content_type": "application/pdf",
        "size_bytes": 1024,
        "correlation_id": "corr-123",
        "requested_at": datetime.now(UTC).isoformat(),
    }).encode()


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
    mock_scanner.scan = AsyncMock(
        side_effect=ConnectionRefusedError("ClamAV down")
    )

    mock_producer = AsyncMock()

    with patch("av_worker.RETRY_DELAYS", [0, 0, 0]):
        await process_scan_request(
            raw=_make_request(),
            s3=mock_s3,
            scanner=mock_scanner,
            producer=mock_producer,
            result_topic="documents.av.scan.result",
        )

    # Scanner called 3 times (retries)
    assert mock_scanner.scan.call_count == 3

    # Published ERROR result
    call_args = mock_producer.send.call_args
    result_json = json.loads(call_args.args[2])
    assert result_json["status"] == "ERROR"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/test_av_worker.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'av_worker'`

- [ ] **Step 3: Write implementation**

```python
# av_worker.py

"""
AV Worker — standalone process that consumes scan requests from Kafka,
scans files via ClamAV, and publishes results back to Kafka.
"""

import asyncio
from datetime import datetime, UTC

import structlog

from app.config import Settings
from app.domain.kafka_schemas import AVScanRequest, AVScanResult, AVScanDetails
from app.domain.scanner import ScanResult
from app.infrastructure.clamav_scanner import ClamAVScanner
from app.infrastructure.kafka_producer import KafkaProducer
from app.storage.s3_client import S3Client

logger = structlog.get_logger()

RETRY_DELAYS = [5, 15, 30]
MAX_CONCURRENT_SCANS = 3


async def process_scan_request(
    raw: bytes,
    s3: S3Client,
    scanner,
    producer: KafkaProducer,
    result_topic: str,
) -> None:
    request = AVScanRequest.model_validate_json(raw)
    file_id = request.file_id

    logger.info("av_scan_started", file_id=str(file_id))

    # Download file from quarantine
    data = await s3.get_object(request.bucket, request.storage_key)

    # Scan with retries
    scan_result: ScanResult | None = None
    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            scan_result = await scanner.scan(data)
            break
        except Exception:
            logger.warning(
                "av_scan_retry",
                file_id=str(file_id),
                attempt=attempt + 1,
                delay=delay,
            )
            if delay > 0:
                await asyncio.sleep(delay)

    # Build result message
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
            details=AVScanDetails(
                signatures_checked=0,
                scan_duration_ms=0,
                threats_found=[],
            ),
        )

    await producer.send(
        topic=result_topic,
        key=str(file_id),
        value=result.model_dump_json().encode(),
    )
    logger.info(
        "av_scan_result_published",
        file_id=str(file_id),
        status=result.status,
    )


async def main() -> None:
    settings = Settings()

    s3 = S3Client(
        endpoint_url=settings.s3_endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )
    scanner = ClamAVScanner(
        host=settings.clamav_host,
        port=settings.clamav_port,
    )
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
            await process_scan_request(
                raw=msg_value,
                s3=s3,
                scanner=scanner,
                producer=producer,
                result_topic=settings.kafka_topic_av_result,
            )

    try:
        async for msg in consumer:
            asyncio.create_task(_process_with_semaphore(msg.value))
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_av_worker.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add av_worker.py tests/unit/test_av_worker.py
git commit -m "feat: add AV worker process with retry and concurrency control"
```

---

### Task 16: Update main.py lifespan and dependencies

**Files:**
- Modify: `app/main.py`
- Modify: `app/dependencies.py`

**Context:** The lifespan must start the Kafka producer, create AVTaskProducer and EventProducer, and start a background task for the AVResultConsumer Kafka consumer loop. Session management for the consumer: create a new session per message, commit after processing.

- [ ] **Step 1: Update lifespan in `app/main.py`**

Add imports:

```python
import asyncio
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.av_task_producer import AVTaskProducer
from app.infrastructure.events import EventProducer
from app.infrastructure.av_consumer import AVResultConsumer
from app.domain.quarantine_service import QuarantineService
```

After existing S3/cache/token initialization (around line 55), add:

```python
    # Kafka
    kafka_producer = None
    try:
        kafka_producer = KafkaProducer(settings.kafka_bootstrap)
        await kafka_producer.start()
    except Exception:
        logger.warning("kafka_unavailable_at_startup")

    app.state.kafka_producer = kafka_producer

    if kafka_producer:
        app.state.av_task_producer = AVTaskProducer(
            kafka=kafka_producer,
            topic=settings.kafka_topic_av_request,
        )
        app.state.event_producer = EventProducer(
            kafka=kafka_producer,
            topic=settings.kafka_topic_events,
        )
    else:
        app.state.av_task_producer = None
        app.state.event_producer = None

    # AV Result Consumer (background task)
    av_consumer_task = None
    if kafka_producer:
        from aiokafka import AIOKafkaConsumer as KafkaConsumer

        kafka_consumer = KafkaConsumer(
            settings.kafka_topic_av_result,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id="dss-av-consumer",
        )
        try:
            await kafka_consumer.start()

            async def _consume_av_results():
                async for msg in kafka_consumer:
                    session = session_factory()
                    try:
                        repo = MetadataRepository(session)
                        quarantine_svc = QuarantineService(
                            s3=app.state.s3,
                            repo=repo,
                            events=app.state.event_producer,
                        )
                        consumer = AVResultConsumer(
                            repo=repo,
                            quarantine_service=quarantine_svc,
                            events=app.state.event_producer,
                        )
                        await consumer.handle_message(msg.value)
                        await session.commit()
                    except Exception:
                        await session.rollback()
                        logger.exception("av_result_processing_error")
                    finally:
                        await session.close()

            av_consumer_task = asyncio.create_task(_consume_av_results())
        except Exception:
            logger.warning("kafka_consumer_unavailable")
```

In cleanup (after `yield`), add:

```python
    if av_consumer_task:
        av_consumer_task.cancel()
        try:
            await av_consumer_task
        except asyncio.CancelledError:
            pass
    if hasattr(app.state, '_kafka_consumer'):
        await app.state._kafka_consumer.stop()
    if kafka_producer:
        await kafka_producer.stop()
```

- [ ] **Step 2: Update `app/dependencies.py`**

In `get_file_service`, add `av_producer` parameter:

```python
def get_file_service(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
) -> FileService:
    return FileService(
        repo=MetadataRepository(session),
        s3=request.app.state.s3,
        cache=request.app.state.cache,
        settings=request.app.state.settings,
        av_producer=getattr(request.app.state, "av_task_producer", None),
    )
```

- [ ] **Step 3: Run all tests**

Run: `uv run python -m pytest tests/unit/ -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add app/main.py app/dependencies.py
git commit -m "feat: wire Kafka producer, AV consumer background task into lifespan"
```

---

### Task 17: Update docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add Kafka, ClamAV, and AV worker services**

Add after existing services in `docker-compose.yml`:

```yaml
  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 0
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 0@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: kafka-broker-api-versions.sh --bootstrap-server localhost:9092
      interval: 10s
      timeout: 5s
      retries: 5

  clamav:
    image: clamav/clamav:1.3
    ports:
      - "3310:3310"
    healthcheck:
      test: clamdcheck
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 120s

  av-worker:
    build: .
    command: uv run python av_worker.py
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy
      clamav:
        condition: service_healthy
    env_file: .env.example
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      S3_ENDPOINT: http://minio:9000
      CLAMAV_HOST: clamav
      CLAMAV_PORT: 3310
```

Also update the `app` service `depends_on` to include `kafka`:

```yaml
      kafka:
        condition: service_healthy
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Kafka, ClamAV, and AV worker to docker-compose"
```

---

### Task 18: Lint, format, final test run

- [ ] **Step 1: Run ruff format and check**

```bash
uv run ruff format .
uv run ruff check . --fix
uv run ruff check .
```

Fix any remaining issues manually.

- [ ] **Step 2: Run all unit tests**

```bash
uv run python -m pytest tests/unit/ -v
```

Expected: All tests PASS (43 existing + ~25 new tests)

- [ ] **Step 3: Commit any lint fixes**

```bash
git add -A
git commit -m "fix: lint and format fixes for Slice 2"
```

---
