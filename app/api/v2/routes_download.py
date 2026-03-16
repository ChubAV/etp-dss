import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService
from app.domain.schemas import PresignedUrlRequest, PresignedUrlResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["download"])


@router.get("/download")
async def download_file(
    request: Request,
    token: str = Query(...),
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
):
    try:
        payload = await request.app.state.download_token_service.consume(token)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))

    url = await file_service.generate_presigned_url(
        file_id=uuid.UUID(payload["file_id"]),
        disposition=payload.get("disposition", "inline"),
    )
    return RedirectResponse(url=url, status_code=302)


@router.post("/{file_id}/presigned-url", response_model=PresignedUrlResponse)
async def create_presigned_url(
    file_id: uuid.UUID,
    body: PresignedUrlRequest,
    request: Request,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.read")),
):
    file = await file_service.get_file(file_id)
    url = await file_service.generate_presigned_url(
        file_id=file_id,
        expires_in=body.expires_in_seconds,
        disposition=body.disposition,
    )
    return PresignedUrlResponse(
        url=url,
        expires_at=datetime.now(UTC) + timedelta(seconds=body.expires_in_seconds),
        file_id=file.id,
        content_type=file.content_type,
        size_bytes=file.size_bytes,
    )
