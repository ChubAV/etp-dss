import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.db_models import File


class MetadataRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, file: File) -> File:
        self._session.add(file)
        await self._session.flush()
        return file

    async def get_by_id(self, file_id: uuid.UUID) -> File | None:
        stmt = select(File).where(File.id == file_id, File.deleted_at.is_(None))
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_owner(
        self,
        owner_type: str,
        owner_id: uuid.UUID,
        av_status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[File], int]:
        base = select(File).where(
            File.owner_type == owner_type,
            File.owner_id == owner_id,
            File.deleted_at.is_(None),
            File.is_latest.is_(True),
        )
        if av_status:
            base = base.where(File.av_status == av_status)

        count_result = await self._session.execute(
            select(func.count()).select_from(base.subquery())
        )
        total = count_result.scalar()

        items_result = await self._session.execute(
            base.offset((page - 1) * page_size).limit(page_size).order_by(File.uploaded_at.desc())
        )
        return list(items_result.scalars().all()), total

    async def soft_delete(self, file_id: uuid.UUID) -> bool:
        stmt = (
            update(File)
            .where(File.id == file_id, File.deleted_at.is_(None))
            .values(deleted_at=datetime.now(timezone.utc))
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount > 0
