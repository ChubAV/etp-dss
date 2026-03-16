from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.v2.exception_handlers import register_exception_handlers
from app.api.v2.routes_public import router as pub_router
from app.dependencies import get_db_session, get_file_service


async def test_public_download_rejects_pending_av():
    file = MagicMock()
    file.id = uuid4()
    file.visibility = "PUBLIC"
    file.av_status = "PENDING"
    mock_service = AsyncMock()
    mock_service.get_file = AsyncMock(return_value=file)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(pub_router)
    app.dependency_overrides[get_file_service] = lambda: mock_service
    app.dependency_overrides[get_db_session] = lambda: AsyncMock()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/api/v2/documents/public/{file.id}/download")
    assert resp.status_code == 403


async def test_public_download_allows_clean_av():
    file = MagicMock()
    file.id = uuid4()
    file.visibility = "PUBLIC"
    file.av_status = "CLEAN"
    mock_service = AsyncMock()
    mock_service.get_file = AsyncMock(return_value=file)
    mock_service.generate_presigned_url = AsyncMock(return_value="https://s3/url")
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(pub_router)
    app.dependency_overrides[get_file_service] = lambda: mock_service
    app.dependency_overrides[get_db_session] = lambda: AsyncMock()
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=False
    ) as client:
        resp = await client.get(f"/api/v2/documents/public/{file.id}/download")
    assert resp.status_code == 302
