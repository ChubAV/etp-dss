import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Request, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService
from app.domain.schemas import FileResponse
from app.infrastructure.auth import decode_service_jwt, verify_scope

router = APIRouter(prefix="/api/v2/documents", tags=["upload"])


@router.post("/upload", response_model=FileResponse, status_code=201)
async def upload_file(
    request: Request,
    file: UploadFile,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    x_upload_token: str | None = Header(None),
    authorization: str | None = Header(None),
    owner_type: str | None = None,
    owner_id: str | None = None,
    visibility: str | None = None,
):
    if x_upload_token:
        svc = request.app.state.upload_token_service
        try:
            payload = await svc.consume(x_upload_token)
        except ValueError as e:
            raise HTTPException(status_code=401, detail=str(e))

        up_owner_type = payload["owner_type"]
        up_owner_id = uuid.UUID(payload["owner_id"])
        up_visibility = payload["visibility"]
        up_uploaded_by = uuid.UUID(payload["uploaded_by"])
        max_size = payload["max_size_bytes"]

        if file.size and file.size > max_size:
            raise HTTPException(
                status_code=413,
                detail=f"File exceeds max size of {max_size} bytes",
            )

    elif authorization and authorization.startswith("Bearer "):
        settings = request.app.state.settings
        try:
            jwt_payload = decode_service_jwt(
                authorization[7:],
                settings.service_jwt_secret,
                settings.service_jwt_algorithm,
            )
            verify_scope(jwt_payload, "documents.write")
        except (ValueError, PermissionError) as e:
            raise HTTPException(status_code=403, detail=str(e))

        if not owner_type or not owner_id:
            raise HTTPException(
                status_code=400,
                detail="owner_type and owner_id required for M2M upload",
            )

        up_owner_type = owner_type
        up_owner_id = uuid.UUID(owner_id)
        up_visibility = visibility or "PRIVATE"
        up_uploaded_by = uuid.UUID(jwt_payload.get("sub", str(uuid.uuid4())))
        max_size = settings.max_file_size_mb * 1024 * 1024
    else:
        raise HTTPException(
            status_code=401,
            detail="X-Upload-Token or Authorization header required",
        )

    correlation_id = getattr(request.state, "correlation_id", None)

    async def file_chunks():
        while chunk := await file.read(64 * 1024):
            yield chunk

    result = await file_service.upload(
        file_stream=file_chunks(),
        file_name=file.filename or "unnamed",
        content_type=file.content_type or "application/octet-stream",
        size_bytes=file.size or 0,
        owner_type=up_owner_type,
        owner_id=up_owner_id,
        visibility=up_visibility,
        uploaded_by=up_uploaded_by,
        correlation_id=correlation_id,
    )
    await session.commit()

    return FileResponse(
        file_id=result.id,
        original_name=result.original_name,
        content_type=result.content_type,
        size_bytes=result.size_bytes,
        checksum_sha256=result.checksum_sha256,
        checksum_gost=result.checksum_gost,
        av_status=result.av_status,
        version=result.version,
        visibility=result.visibility,
        owner_type=result.owner_type,
        owner_id=result.owner_id,
        uploaded_by=result.uploaded_by,
        uploaded_at=result.uploaded_at,
    )
