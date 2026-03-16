import uuid

import pytest
from pydantic import ValidationError

from app.domain.schemas import DownloadTokenRequest, FileResponse, UploadTokenRequest


def test_upload_token_request_valid():
    req = UploadTokenRequest(
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        visibility="PUBLIC",
        file_name="test.pdf",
        content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024,
        uploaded_by=uuid.uuid4(),
    )
    assert req.owner_type == "LOT"


def test_upload_token_request_invalid_visibility():
    with pytest.raises(ValidationError):
        UploadTokenRequest(
            owner_type="LOT",
            owner_id=uuid.uuid4(),
            visibility="INVALID",
            file_name="t.pdf",
            content_type="application/pdf",
            max_size_bytes=1024,
            uploaded_by=uuid.uuid4(),
        )


def test_file_response_includes_uploaded_by():
    resp = FileResponse(
        file_id=uuid.uuid4(),
        original_name="test.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        checksum_sha256="abc",
        checksum_gost="def",
        av_status="PENDING",
        version=1,
        visibility="PRIVATE",
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        uploaded_by=uuid.uuid4(),
        uploaded_at="2025-01-01T00:00:00Z",
    )
    data = resp.model_dump(mode="json")
    assert "uploaded_by" in data


def test_download_token_request_defaults():
    req = DownloadTokenRequest(file_id=uuid.uuid4(), user_id=uuid.uuid4())
    assert req.disposition == "inline"
    assert req.expires_in_seconds == 300
