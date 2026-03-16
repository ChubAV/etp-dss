import os
from datetime import UTC, datetime, timedelta

import jwt
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://dss:password@localhost:5432/document_storage"
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("S3_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("S3_ACCESS_KEY", "minioadmin")
os.environ.setdefault("S3_SECRET_KEY", "minioadmin")
os.environ.setdefault("SERVICE_JWT_SECRET", "test-service-jwt-secret")
os.environ.setdefault("UPLOAD_TOKEN_SECRET", "test-upload-token-secret")
os.environ.setdefault("DOWNLOAD_TOKEN_SECRET", "test-download-token-secret")


@pytest_asyncio.fixture
async def client():
    from app.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
def service_token():
    return jwt.encode(
        {
            "sub": "test-service",
            "scopes": [
                "documents.issue_token",
                "documents.write",
                "documents.read",
                "documents.delete",
            ],
            "iat": datetime.now(UTC),
            "exp": datetime.now(UTC) + timedelta(hours=1),
        },
        os.environ["SERVICE_JWT_SECRET"],
        algorithm="HS256",
    )
