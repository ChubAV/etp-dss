import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService
from app.domain.schemas import FileListResponse, FileResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["metadata"])


@router.get("/{file_id}", response_model=FileResponse)
async def get_file_metadata(
    file_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.read")),
):
    f = await file_service.get_file(file_id)
    return FileResponse(
        file_id=f.id,
        original_name=f.original_name,
        content_type=f.content_type,
        size_bytes=f.size_bytes,
        checksum_sha256=f.checksum_sha256,
        checksum_gost=f.checksum_gost,
        av_status=f.av_status,
        version=f.version,
        visibility=f.visibility,
        owner_type=f.owner_type,
        owner_id=f.owner_id,
        uploaded_by=f.uploaded_by,
        uploaded_at=f.uploaded_at,
    )


@router.get("/by-owner/{owner_type}/{owner_id}", response_model=FileListResponse)
async def get_files_by_owner(
    owner_type: str,
    owner_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.read")),
    av_status: str | None = Query(None),
    include_signatures: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    items, total = await file_service.get_files_by_owner(
        owner_type=owner_type,
        owner_id=owner_id,
        av_status=av_status,
        page=page,
        page_size=page_size,
    )
    return FileListResponse(
        items=[
            FileResponse(
                file_id=f.id,
                original_name=f.original_name,
                content_type=f.content_type,
                size_bytes=f.size_bytes,
                checksum_sha256=f.checksum_sha256,
                checksum_gost=f.checksum_gost,
                av_status=f.av_status,
                version=f.version,
                visibility=f.visibility,
                owner_type=f.owner_type,
                owner_id=f.owner_id,
                uploaded_by=f.uploaded_by,
                uploaded_at=f.uploaded_at,
            )
            for f in items
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.delete("/{file_id}", status_code=204)
async def delete_file(
    file_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.delete")),
):
    await file_service.soft_delete(file_id)
    await session.commit()
