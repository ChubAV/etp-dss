from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.file_service import FileService
from app.storage.metadata_repository import MetadataRepository


async def get_db_session(request: Request):
    session_factory = request.app.state.db_session_factory
    async with session_factory() as session:
        yield session


async def get_file_service(
    request: Request,
    session: AsyncSession = Depends(get_db_session),
) -> FileService:
    return FileService(
        repo=MetadataRepository(session),
        s3=request.app.state.s3,
        cache=request.app.state.cache,
        settings=request.app.state.settings,
    )
