import uuid as uuid_mod
from collections.abc import AsyncIterator

import structlog

from app.config import Settings
from app.domain.db_models import File
from app.domain.exceptions import FileNotFoundError
from app.domain.hash_calculator import compute_hashes
from app.domain.validators import validate_content_type, validate_file_size
from app.storage.cache_client import CacheClient
from app.storage.metadata_repository import MetadataRepository
from app.storage.s3_client import S3Client

logger = structlog.get_logger()


class FileService:
    def __init__(
        self,
        repo: MetadataRepository,
        s3: S3Client,
        cache: CacheClient,
        settings: Settings,
        av_producer=None,
    ):
        self._repo = repo
        self._s3 = s3
        self._cache = cache
        self._settings = settings
        self._av_producer = av_producer

    async def upload(
        self,
        file_stream: AsyncIterator[bytes],
        file_name: str,
        content_type: str,
        size_bytes: int,
        owner_type: str,
        owner_id: uuid_mod.UUID,
        visibility: str,
        uploaded_by: uuid_mod.UUID,
        correlation_id: str | None = None,
    ) -> File:
        validate_content_type(content_type, self._settings.allowed_content_types)
        validate_file_size(size_bytes, self._settings.max_file_size_mb)

        # Collect file and compute hashes in one pass.
        # TODO: For true streaming to S3, use multipart upload with temp file.
        # Current approach loads file into memory — acceptable for <=20MB files
        # but should be optimized for production load (500 concurrent uploads).
        collected = bytearray()

        async def collecting_chunks():
            async for chunk in file_stream:
                collected.extend(chunk)
                yield chunk

        sha256_hex, gost_hex = await compute_hashes(collecting_chunks())
        file_bytes = bytes(collected)

        # Target bucket (where file goes after AV scan)
        target_bucket = (
            self._settings.s3_bucket_public
            if visibility == "PUBLIC"
            else self._settings.s3_bucket_private
        )
        file_id = uuid_mod.uuid4()
        target_key = f"{owner_type.lower()}/{owner_id}/{file_id}/1"

        # Upload to quarantine
        quarantine_bucket = self._settings.s3_bucket_quarantine
        quarantine_key = str(file_id)

        s3_version_id = await self._s3.upload_object(
            bucket=quarantine_bucket,
            key=quarantine_key,
            body=file_bytes,
            content_type=content_type,
        )

        file = File(
            original_name=file_name,
            storage_key=quarantine_key,
            bucket=quarantine_bucket,
            content_type=content_type,
            size_bytes=len(file_bytes),
            checksum_sha256=sha256_hex,
            checksum_gost=gost_hex,
            owner_type=owner_type,
            owner_id=owner_id,
            uploaded_by=uploaded_by,
        )
        file.id = file_id
        file.s3_version_id = s3_version_id
        file.visibility = visibility
        file.correlation_id = uuid_mod.UUID(correlation_id) if correlation_id else None
        file.target_bucket = target_bucket
        file.target_key = target_key
        file.av_status = "SCANNING"

        file = await self._repo.create(file)

        # Publish AV scan request
        if self._av_producer:
            await self._av_producer.publish(
                file_id=file_id,
                storage_key=quarantine_key,
                bucket=quarantine_bucket,
                content_type=content_type,
                size_bytes=len(file_bytes),
                correlation_id=str(correlation_id) if correlation_id else "",
            )

        logger.info("file_uploaded", file_id=str(file.id), size=len(file_bytes))
        return file

    async def get_file(self, file_id: uuid_mod.UUID) -> File:
        file = await self._repo.get_by_id(file_id)
        if not file:
            raise FileNotFoundError(f"File {file_id} not found")
        return file

    async def get_files_by_owner(
        self,
        owner_type: str,
        owner_id: uuid_mod.UUID,
        av_status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[File], int]:
        return await self._repo.get_by_owner(
            owner_type=owner_type,
            owner_id=owner_id,
            av_status=av_status,
            page=page,
            page_size=min(page_size, 100),
        )

    async def soft_delete(self, file_id: uuid_mod.UUID) -> None:
        if not await self._repo.soft_delete(file_id):
            raise FileNotFoundError(f"File {file_id} not found")
        logger.info("file_deleted", file_id=str(file_id))

    async def generate_presigned_url(
        self,
        file_id: uuid_mod.UUID,
        expires_in: int | None = None,
        disposition: str = "inline",
    ) -> str:
        file = await self.get_file(file_id)
        ttl = expires_in or (
            self._settings.public_presigned_url_ttl_seconds
            if file.visibility == "PUBLIC"
            else self._settings.presigned_url_ttl_seconds
        )

        cached = await self._cache.get_cached_presigned_url(str(file_id), disposition)
        if cached:
            return cached

        url = await self._s3.generate_presigned_url(
            bucket=file.bucket,
            key=file.storage_key,
            expires_in=ttl,
            disposition=disposition,
        )
        await self._cache.cache_presigned_url(
            str(file_id), disposition, url, ttl_seconds=max(ttl - 60, 60)
        )
        return url
