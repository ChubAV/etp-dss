from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Request

from app.domain.schemas import UploadTokenRequest, UploadTokenResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["upload-token"])


@router.post("/upload-token", response_model=UploadTokenResponse, status_code=201)
async def create_upload_token(
    body: UploadTokenRequest,
    request: Request,
    _: dict = Depends(require_scope("documents.issue_token")),
):
    service = request.app.state.upload_token_service
    settings = request.app.state.settings
    token = service.generate(
        owner_type=body.owner_type,
        owner_id=body.owner_id,
        visibility=body.visibility.value,
        file_name=body.file_name,
        content_type=body.content_type,
        max_size_bytes=body.max_size_bytes,
        uploaded_by=body.uploaded_by,
    )
    return UploadTokenResponse(
        upload_token=token,
        expires_at=datetime.now(UTC) + timedelta(seconds=settings.upload_token_ttl_seconds),
        owner_type=body.owner_type,
        owner_id=body.owner_id,
    )
