import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Visibility(str, Enum):
    PRIVATE = "PRIVATE"
    PUBLIC = "PUBLIC"


class AVStatus(str, Enum):
    PENDING = "PENDING"
    SCANNING = "SCANNING"
    CLEAN = "CLEAN"
    INFECTED = "INFECTED"
    ERROR = "ERROR"


class UploadTokenRequest(BaseModel):
    owner_type: str
    owner_id: uuid.UUID
    visibility: Visibility
    file_name: str
    content_type: str
    max_size_bytes: int = Field(gt=0)
    uploaded_by: uuid.UUID


class UploadTokenResponse(BaseModel):
    upload_token: str
    expires_at: datetime
    owner_type: str
    owner_id: uuid.UUID


class DownloadTokenRequest(BaseModel):
    file_id: uuid.UUID
    user_id: uuid.UUID
    version: int | None = None
    disposition: str = "inline"
    expires_in_seconds: int = Field(default=300, gt=0, le=600)


class DownloadTokenResponse(BaseModel):
    download_token: str
    expires_at: datetime
    file_id: uuid.UUID
    original_name: str
    content_type: str
    size_bytes: int


class FileResponse(BaseModel):
    file_id: uuid.UUID
    original_name: str
    content_type: str
    size_bytes: int
    checksum_sha256: str
    checksum_gost: str
    av_status: str
    version: int
    visibility: str
    owner_type: str
    owner_id: uuid.UUID
    uploaded_by: uuid.UUID
    uploaded_at: datetime


class FileListResponse(BaseModel):
    items: list[FileResponse]
    total: int
    page: int
    page_size: int


class PresignedUrlRequest(BaseModel):
    expires_in_seconds: int = Field(default=300, gt=0, le=600)
    disposition: str = "inline"


class PresignedUrlResponse(BaseModel):
    url: str
    expires_at: datetime
    file_id: uuid.UUID
    content_type: str
    size_bytes: int
