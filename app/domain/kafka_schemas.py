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
