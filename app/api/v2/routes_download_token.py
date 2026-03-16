from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService
from app.domain.schemas import DownloadTokenRequest, DownloadTokenResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["download-token"])


@router.post("/download-token", response_model=DownloadTokenResponse, status_code=201)
async def create_download_token(
    body: DownloadTokenRequest,
    request: Request,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.issue_token")),
):
    file = await file_service.get_file(body.file_id)

    token = request.app.state.download_token_service.generate(
        file_id=body.file_id,
        user_id=body.user_id,
        version=body.version,
        disposition=body.disposition,
        expires_in_seconds=body.expires_in_seconds,
    )
    return DownloadTokenResponse(
        download_token=token,
        expires_at=datetime.now(UTC) + timedelta(seconds=body.expires_in_seconds),
        file_id=file.id,
        original_name=file.original_name,
        content_type=file.content_type,
        size_bytes=file.size_bytes,
    )
