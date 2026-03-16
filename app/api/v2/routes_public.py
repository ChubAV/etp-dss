import uuid

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService

router = APIRouter(prefix="/api/v2/documents", tags=["public"])


@router.get("/public/{file_id}/download")
async def download_public_file(
    file_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
):
    file = await file_service.get_file(file_id)
    if file.visibility != "PUBLIC":
        raise HTTPException(status_code=404, detail="File not found")
    url = await file_service.generate_presigned_url(file_id=file_id)
    return RedirectResponse(url=url, status_code=302)
