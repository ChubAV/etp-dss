import os
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("SERVICE_JWT_SECRET", "test")
os.environ.setdefault("UPLOAD_TOKEN_SECRET", "test")
os.environ.setdefault("DOWNLOAD_TOKEN_SECRET", "test")

from fastapi.testclient import TestClient


def test_health_liveness():
    with (
        patch("app.main.create_async_engine"),
        patch("app.main.async_sessionmaker"),
        patch("app.main.Redis.from_url", return_value=AsyncMock()),
        patch("app.main.S3Client", return_value=MagicMock()),
        patch("app.main.CacheClient", return_value=MagicMock()),
        patch("app.main.UploadTokenService", return_value=MagicMock()),
        patch("app.main.DownloadTokenService", return_value=MagicMock()),
    ):
        from app.main import app
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
