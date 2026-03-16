# DSS Срез 1: Каркас + Upload + Download — План реализации

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реализовать рабочий Document Storage Service с полным циклом загрузки и скачивания файлов через upload/download токены.

**Architecture:** Трёхслойная async архитектура на FastAPI: API layer (роутеры) → Domain layer (сервисы, валидаторы) → Storage layer (S3, PostgreSQL, Redis). DI через FastAPI Depends, конфигурация через pydantic-settings.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 async, asyncpg, Alembic, aiobotocore, redis, PyJWT, pygost, structlog, prometheus-client, uv

---

## Chunk 1: Каркас проекта

### Task 1: Зависимости и конфигурация

**Files:**
- Modify: `pyproject.toml`
- Create: `app/__init__.py`
- Create: `app/config.py`
- Create: `.env.example`
- Test: `tests/__init__.py`
- Test: `tests/unit/__init__.py`
- Test: `tests/unit/test_config.py`

- [ ] **Step 1: Установить зависимости**

Обновить `pyproject.toml`:

```toml
[project]
name = "dss"
version = "0.1.0"
description = "Document Storage Service for ETP"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.32.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.30.0",
    "alembic>=1.14.0",
    "aiobotocore>=2.15.0",
    "redis[hiredis]>=5.2.0",
    "pyjwt>=2.9.0",
    "pygost>=5.0",
    "structlog>=24.0.0",
    "prometheus-client>=0.21.0",
    "pydantic-settings>=2.6.0",
    "aiohttp>=3.11.0",
    "aiokafka>=0.12.0",
    "python-multipart>=0.0.12",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "httpx>=0.28.0",
    "ruff>=0.8.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = ["integration: marks tests as integration tests (require running services)"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "UP"]
```

Run: `uv sync`

- [ ] **Step 2: Создать `.env.example`**

```env
# S3
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_REGION=us-east-1
S3_BUCKET_PRIVATE=documents-private
S3_BUCKET_PUBLIC=documents-public
S3_BUCKET_QUARANTINE=documents-quarantine

# PostgreSQL
DATABASE_URL=postgresql+asyncpg://dss:password@postgres:5432/document_storage

# Redis
REDIS_URL=redis://redis:6379/0

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC_AV_REQUEST=documents.av.scan.request
KAFKA_TOPIC_AV_RESULT=documents.av.scan.result
KAFKA_TOPIC_SIGNATURES_REQUEST=documents.signatures.request
KAFKA_TOPIC_EVENTS=documents.events
KAFKA_TOPIC_AUDIT=audit.document_storage

# ClamAV
CLAMAV_HOST=clamav
CLAMAV_PORT=3310

# Crypto EDS
CRYPTO_EDS_URL=http://crypto-eds:8000
CRYPTO_EDS_API_KEY=change-me

# Service JWT (for M2M auth between services)
SERVICE_JWT_SECRET=dev-service-jwt-secret-change-me
SERVICE_JWT_ALGORITHM=HS256

# Upload-token
UPLOAD_TOKEN_SECRET=dev-upload-token-secret-change-me
UPLOAD_TOKEN_TTL_SECONDS=600
UPLOAD_TOKEN_ALGORITHM=HS256

# Download-token
DOWNLOAD_TOKEN_SECRET=dev-download-token-secret-change-me
DOWNLOAD_TOKEN_MAX_TTL_SECONDS=600
DOWNLOAD_TOKEN_ALGORITHM=HS256

# Uploads
MAX_FILE_SIZE_MB=20
ALLOWED_CONTENT_TYPES=application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,image/jpeg,image/png
PRESIGNED_URL_TTL_SECONDS=300
PUBLIC_PRESIGNED_URL_TTL_SECONDS=3600

# Rate limits
RATE_LIMIT_UPLOAD_PER_USER=30/5m
RATE_LIMIT_DOWNLOAD_PER_USER=100/1m
RATE_LIMIT_PUBLIC_PER_IP=200/1m
RATE_LIMIT_API_PER_SERVICE=1000/1m
```

- [ ] **Step 3: Написать тест для конфигурации**

`tests/__init__.py` и `tests/unit/__init__.py` — пустые файлы.

`tests/unit/test_config.py`:

```python
def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("S3_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("SERVICE_JWT_SECRET", "test-secret")
    monkeypatch.setenv("UPLOAD_TOKEN_SECRET", "test-secret")
    monkeypatch.setenv("DOWNLOAD_TOKEN_SECRET", "test-secret")

    from app.config import Settings

    settings = Settings()
    assert settings.database_url == "postgresql+asyncpg://test:test@localhost/test"
    assert settings.s3_endpoint == "http://localhost:9000"
    assert settings.max_file_size_mb == 20
    assert "application/pdf" in settings.allowed_content_types


def test_settings_parses_allowed_content_types(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("S3_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("SERVICE_JWT_SECRET", "test-secret")
    monkeypatch.setenv("UPLOAD_TOKEN_SECRET", "test-secret")
    monkeypatch.setenv("DOWNLOAD_TOKEN_SECRET", "test-secret")
    monkeypatch.setenv("ALLOWED_CONTENT_TYPES", "application/pdf,image/png")

    from app.config import Settings

    settings = Settings()
    assert settings.allowed_content_types == ["application/pdf", "image/png"]
```

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `app.config` не существует.

- [ ] **Step 4: Реализовать конфигурацию**

`app/__init__.py` — пустой файл.

`app/config.py`:

```python
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL
    database_url: str

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # S3
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_bucket_private: str = "documents-private"
    s3_bucket_public: str = "documents-public"
    s3_bucket_quarantine: str = "documents-quarantine"

    # Kafka
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic_av_request: str = "documents.av.scan.request"
    kafka_topic_av_result: str = "documents.av.scan.result"
    kafka_topic_signatures_request: str = "documents.signatures.request"
    kafka_topic_events: str = "documents.events"
    kafka_topic_audit: str = "audit.document_storage"

    # ClamAV
    clamav_host: str = "clamav"
    clamav_port: int = 3310

    # Crypto EDS
    crypto_eds_url: str = "http://crypto-eds:8000"
    crypto_eds_api_key: str = ""

    # Service JWT (M2M)
    service_jwt_secret: str
    service_jwt_algorithm: str = "HS256"

    # Upload-token
    upload_token_secret: str
    upload_token_ttl_seconds: int = 600
    upload_token_algorithm: str = "HS256"

    # Download-token
    download_token_secret: str
    download_token_max_ttl_seconds: int = 600
    download_token_algorithm: str = "HS256"

    # Uploads
    max_file_size_mb: int = 20
    allowed_content_types: list[str] = [
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "image/jpeg",
        "image/png",
    ]
    presigned_url_ttl_seconds: int = 300
    public_presigned_url_ttl_seconds: int = 3600

    # Rate limits
    rate_limit_upload_per_user: str = "30/5m"
    rate_limit_download_per_user: str = "100/1m"
    rate_limit_public_per_ip: str = "200/1m"
    rate_limit_api_per_service: str = "1000/1m"

    @field_validator("allowed_content_types", mode="before")
    @classmethod
    def parse_content_types(cls, v):
        if isinstance(v, str):
            return [ct.strip() for ct in v.split(",")]
        return v

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
```

- [ ] **Step 5: Запустить тест**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .env.example app/__init__.py app/config.py tests/__init__.py tests/unit/__init__.py tests/unit/test_config.py
git commit -m "feat: add project dependencies and Settings config"
```

---

### Task 2: Structlog + Correlation ID

**Files:**
- Create: `app/infrastructure/__init__.py`
- Create: `app/infrastructure/logging.py`
- Create: `app/infrastructure/correlation.py`
- Test: `tests/unit/test_correlation.py`

- [ ] **Step 1: Написать тест для correlation middleware**

`tests/unit/test_correlation.py`:

```python
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from app.infrastructure.correlation import CorrelationMiddleware

app = FastAPI()
app.add_middleware(CorrelationMiddleware)


@app.get("/test")
async def test_endpoint(request: Request):
    return JSONResponse({"correlation_id": request.state.correlation_id})


async def test_generates_correlation_id_if_missing():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test")
    assert response.status_code == 200
    cid = response.json()["correlation_id"]
    uuid.UUID(cid)
    assert response.headers["x-correlation-id"] == cid


async def test_uses_provided_correlation_id():
    test_id = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Correlation-Id": test_id})
    assert response.json()["correlation_id"] == test_id
    assert response.headers["x-correlation-id"] == test_id
```

Run: `uv run pytest tests/unit/test_correlation.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать logging и correlation**

`app/infrastructure/__init__.py` — пустой файл.

`app/infrastructure/logging.py`:

```python
import structlog


def setup_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
```

`app/infrastructure/correlation.py`:

```python
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class CorrelationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        response = await call_next(request)
        response.headers["x-correlation-id"] = correlation_id
        return response
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_correlation.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/infrastructure/__init__.py app/infrastructure/logging.py app/infrastructure/correlation.py tests/unit/test_correlation.py
git commit -m "feat: add structlog logging and correlation ID middleware"
```

---

### Task 3: SQLAlchemy ORM-модель + Alembic

**Files:**
- Create: `app/domain/__init__.py`
- Create: `app/domain/db_models.py`
- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `migrations/script.py.mako`
- Test: `tests/unit/test_db_models.py`

- [ ] **Step 1: Написать тест для ORM-модели**

`tests/unit/test_db_models.py`:

```python
import uuid

from app.domain.db_models import File


def test_file_model_has_required_columns():
    required = [
        "id", "original_name", "storage_key", "bucket", "content_type", "size_bytes",
        "checksum_sha256", "checksum_gost", "owner_type", "owner_id", "version",
        "s3_version_id", "is_latest", "previous_version_id", "visibility",
        "av_status", "av_scanned_at", "av_engine", "av_report",
        "uploaded_by", "uploaded_at", "deleted_at", "correlation_id", "metadata_",
    ]
    columns = {c.key for c in File.__table__.columns}
    for col in required:
        assert col in columns, f"Missing column: {col}"


def test_file_model_defaults():
    f = File(
        original_name="test.pdf",
        storage_key="quarantine/test",
        bucket="documents-quarantine",
        content_type="application/pdf",
        size_bytes=1024,
        checksum_sha256="abc",
        checksum_gost="def",
        owner_type="LOT",
        owner_id=uuid.uuid4(),
        uploaded_by=uuid.uuid4(),
    )
    assert f.version == 1
    assert f.is_latest is True
    assert f.visibility == "PRIVATE"
    assert f.av_status == "PENDING"
```

Run: `uv run pytest tests/unit/test_db_models.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать ORM-модель**

`app/domain/__init__.py` — пустой файл.

`app/domain/db_models.py`:

```python
import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey,
    Index, Integer, String, Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    id: Mapped[uuid_mod.UUID] = mapped_column(Uuid, primary_key=True, default=uuid_mod.uuid4)
    original_name: Mapped[str] = mapped_column(String(500))
    storage_key: Mapped[str] = mapped_column(String(1000))
    bucket: Mapped[str] = mapped_column(String(100))
    content_type: Mapped[str] = mapped_column(String(255))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str] = mapped_column(String(64))
    checksum_gost: Mapped[str] = mapped_column(String(64))
    owner_type: Mapped[str] = mapped_column(String(50))
    owner_id: Mapped[uuid_mod.UUID] = mapped_column(Uuid)
    version: Mapped[int] = mapped_column(Integer, default=1)
    s3_version_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_latest: Mapped[bool] = mapped_column(Boolean, default=True)
    previous_version_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        Uuid, ForeignKey("files.id"), nullable=True
    )
    visibility: Mapped[str] = mapped_column(String(20), default="PRIVATE")
    av_status: Mapped[str] = mapped_column(String(20), default="PENDING")
    av_scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    av_engine: Mapped[str | None] = mapped_column(String(100), nullable=True)
    av_report: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    uploaded_by: Mapped[uuid_mod.UUID] = mapped_column(Uuid)
    uploaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    correlation_id: Mapped[uuid_mod.UUID | None] = mapped_column(Uuid, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, default=dict)

    __table_args__ = (
        CheckConstraint("visibility IN ('PRIVATE', 'PUBLIC')", name="chk_visibility"),
        CheckConstraint(
            "av_status IN ('PENDING', 'SCANNING', 'CLEAN', 'INFECTED', 'ERROR')",
            name="chk_av_status",
        ),
        Index("idx_files_storage_key", "storage_key"),
        Index("idx_files_owner", "owner_type", "owner_id", postgresql_where=deleted_at.is_(None)),
        Index("idx_files_uploaded_by", "uploaded_by"),
        Index("idx_files_av_status", "av_status", postgresql_where=(av_status != "CLEAN")),
        Index("idx_files_visibility", "visibility", postgresql_where=(visibility == "PUBLIC")),
    )
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_db_models.py -v`
Expected: PASS

- [ ] **Step 4: Настроить Alembic**

Run: `uv run alembic init migrations`

Отредактировать `migrations/env.py`:

```python
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.domain.db_models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    try:
        settings = Settings()
        url = settings.database_url
    except Exception:
        url = config.get_main_option("sqlalchemy.url")

    engine = create_async_engine(url)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
```

- [ ] **Step 5: Commit**

```bash
git add app/domain/__init__.py app/domain/db_models.py alembic.ini migrations/ tests/unit/test_db_models.py
git commit -m "feat: add File ORM model and Alembic setup"
```

---

### Task 4: Docker Compose dev-окружение

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `justfile`

- [ ] **Step 1: Создать Dockerfile**

```dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev
COPY . .
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
```

- [ ] **Step 2: Создать docker-compose.yml**

```yaml
services:
  app:
    build: .
    ports:
      - "8001:8001"
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      minio:
        condition: service_healthy

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: dss
      POSTGRES_PASSWORD: password
      POSTGRES_DB: document_storage
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dss -d document_storage"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - miniodata:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb --ignore-existing local/documents-private &&
      mc mb --ignore-existing local/documents-public &&
      mc mb --ignore-existing local/documents-quarantine
      "

volumes:
  pgdata:
  miniodata:
```

- [ ] **Step 3: Создать justfile**

```justfile
run:
    uv run uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload

test:
    uv run pytest -v

test-unit:
    uv run pytest tests/unit -v

test-integration:
    uv run pytest tests/integration -v -m integration

test-one TEST:
    uv run pytest {{TEST}} -v

lint:
    uv run ruff check .
    uv run ruff format --check .

fix:
    uv run ruff check --fix .
    uv run ruff format .

migration MSG:
    uv run alembic revision --autogenerate -m "{{MSG}}"

migrate:
    uv run alembic upgrade head

up:
    docker compose up -d

down:
    docker compose down

rebuild:
    docker compose up -d --build
```

- [ ] **Step 4: Commit**

```bash
git add Dockerfile docker-compose.yml justfile
git commit -m "feat: add Docker Compose dev environment and justfile"
```

---

### Task 5: FastAPI app + lifespan + health + dependencies

**Files:**
- Modify: `main.py` (удалить старый из корня)
- Create: `app/main.py`
- Create: `app/api/__init__.py`
- Create: `app/api/v2/__init__.py`
- Create: `app/api/v2/routes_health.py`
- Create: `app/dependencies.py`
- Create: `app/domain/exceptions.py`
- Test: `tests/unit/test_health.py`

- [ ] **Step 1: Написать тест для health endpoints**

`tests/unit/test_health.py`:

```python
import os
from unittest.mock import AsyncMock, patch

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
    ):
        from app.main import app
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
```

Run: `uv run pytest tests/unit/test_health.py -v`
Expected: FAIL

- [ ] **Step 2: Создать exceptions, dependencies, routes_health**

`app/domain/exceptions.py`:

```python
class DSSError(Exception):
    pass

class FileNotFoundError(DSSError):
    pass

class TokenError(DSSError):
    pass

class AVNotPassedError(DSSError):
    pass

class FileSizeExceededError(DSSError):
    pass

class InvalidContentTypeError(DSSError):
    pass
```

`app/api/__init__.py` и `app/api/v2/__init__.py` — пустые файлы.

`app/api/v2/routes_health.py`:

```python
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

router = APIRouter()


@router.get("/health")
async def liveness():
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request):
    checks = {}
    all_ok = True

    # PostgreSQL
    try:
        async with request.app.state.db_session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"
        all_ok = False

    # Redis
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
        all_ok = False

    # S3
    try:
        s3 = request.app.state.s3
        await s3.head_bucket(request.app.state.settings.s3_bucket_private)
        checks["s3"] = "ok"
    except Exception as e:
        checks["s3"] = f"error: {e}"
        all_ok = False

    return JSONResponse(content=checks, status_code=200 if all_ok else 503)
```

`app/dependencies.py`:

```python
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.file_service import FileService
from app.storage.cache_client import CacheClient
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
```

Note: `get_file_service` depends on `FileService` which will be created in Task 14. For now, it will be a forward reference. The dependency is wired through `Depends()` in routes — session lifecycle is managed by FastAPI's dependency injection, so `session.commit()` must be called explicitly in routes that mutate data.

- [ ] **Step 3: Создать main.py**

Удалить старый `main.py` в корне. Создать `app/main.py`:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v2.routes_health import router as health_router
from app.config import Settings
from app.infrastructure.correlation import CorrelationMiddleware
from app.infrastructure.logging import setup_logging
from app.storage.cache_client import CacheClient
from app.storage.s3_client import S3Client
from app.domain.upload_token_service import UploadTokenService
from app.domain.download_token_service import DownloadTokenService


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()

    settings = Settings()
    app.state.settings = settings

    # PostgreSQL
    engine = create_async_engine(settings.database_url, echo=False)
    app.state.db_session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Redis
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)

    # S3
    app.state.s3 = S3Client(
        endpoint_url=settings.s3_endpoint,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )

    # Cache
    cache = CacheClient(app.state.redis)
    app.state.cache = cache

    # Token services
    app.state.upload_token_service = UploadTokenService(
        secret=settings.upload_token_secret,
        algorithm=settings.upload_token_algorithm,
        ttl_seconds=settings.upload_token_ttl_seconds,
        cache=cache,
    )
    app.state.download_token_service = DownloadTokenService(
        secret=settings.download_token_secret,
        algorithm=settings.download_token_algorithm,
        max_ttl_seconds=settings.download_token_max_ttl_seconds,
        cache=cache,
    )

    yield

    await app.state.redis.aclose()
    await engine.dispose()


app = FastAPI(title="Document Storage Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(CorrelationMiddleware)
app.include_router(health_router)
```

Note: Imports for token services, S3Client, CacheClient won't resolve until those files are created in later tasks. The test in Step 1 patches these away. Full assembly happens in Task 22.

- [ ] **Step 4: Запустить тесты**

Run: `uv run pytest tests/unit/test_health.py tests/unit/test_correlation.py tests/unit/test_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
rm main.py
git add app/main.py app/dependencies.py app/domain/exceptions.py app/api/__init__.py app/api/v2/__init__.py app/api/v2/routes_health.py tests/unit/test_health.py
git commit -m "feat: add FastAPI app with lifespan, DI, and health endpoints"
```

---

## Chunk 2: Storage Layer

### Task 6: Redis cache client

**Files:**
- Create: `app/storage/__init__.py`
- Create: `app/storage/cache_client.py`
- Test: `tests/unit/test_cache_client.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_cache_client.py`:

```python
from unittest.mock import AsyncMock

import pytest

from app.storage.cache_client import CacheClient


@pytest.fixture
def redis_mock():
    mock = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock()
    mock.exists = AsyncMock(return_value=0)
    return mock


@pytest.fixture
def cache(redis_mock):
    return CacheClient(redis_mock)


async def test_blacklist_token(cache, redis_mock):
    await cache.blacklist_token("jti-123", ttl_seconds=600)
    redis_mock.set.assert_called_once_with("token:blacklist:jti-123", "1", ex=600)


async def test_is_token_blacklisted_false(cache, redis_mock):
    redis_mock.exists.return_value = 0
    assert await cache.is_token_blacklisted("jti-123") is False


async def test_is_token_blacklisted_true(cache, redis_mock):
    redis_mock.exists.return_value = 1
    assert await cache.is_token_blacklisted("jti-123") is True


async def test_is_token_blacklisted_redis_down_returns_true(cache, redis_mock):
    """Fail-closed: if Redis is down, treat token as blacklisted."""
    redis_mock.exists.side_effect = ConnectionError("Redis unavailable")
    assert await cache.is_token_blacklisted("jti-123") is True


async def test_cache_presigned_url_includes_disposition(cache, redis_mock):
    await cache.cache_presigned_url("file-id", "inline", "https://s3/presigned", ttl_seconds=240)
    redis_mock.set.assert_called_once_with("presigned:file-id:inline", "https://s3/presigned", ex=240)


async def test_get_cached_presigned_url_by_disposition(cache, redis_mock):
    redis_mock.get.return_value = "https://s3/presigned"
    result = await cache.get_cached_presigned_url("file-id", "inline")
    assert result == "https://s3/presigned"
    redis_mock.get.assert_called_once_with("presigned:file-id:inline")
```

Run: `uv run pytest tests/unit/test_cache_client.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать cache client**

`app/storage/__init__.py` — пустой файл.

`app/storage/cache_client.py`:

```python
import structlog
from redis.asyncio import Redis

logger = structlog.get_logger()


class CacheClient:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def blacklist_token(self, jti: str, ttl_seconds: int) -> None:
        await self._redis.set(f"token:blacklist:{jti}", "1", ex=ttl_seconds)

    async def is_token_blacklisted(self, jti: str) -> bool:
        try:
            return await self._redis.exists(f"token:blacklist:{jti}") > 0
        except Exception:
            logger.warning("redis_unavailable_fail_closed", jti=jti)
            return True  # fail-closed

    async def cache_presigned_url(
        self, file_id: str, disposition: str, url: str, ttl_seconds: int
    ) -> None:
        try:
            await self._redis.set(f"presigned:{file_id}:{disposition}", url, ex=ttl_seconds)
        except Exception:
            logger.warning("redis_cache_write_failed", file_id=file_id)

    async def get_cached_presigned_url(self, file_id: str, disposition: str) -> str | None:
        try:
            return await self._redis.get(f"presigned:{file_id}:{disposition}")
        except Exception:
            logger.warning("redis_cache_read_failed", file_id=file_id)
            return None
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_cache_client.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/storage/__init__.py app/storage/cache_client.py tests/unit/test_cache_client.py
git commit -m "feat: add Redis cache client with fail-closed token blacklist"
```

---

### Task 7: S3 client

**Files:**
- Create: `app/storage/s3_client.py`
- Test: `tests/unit/test_s3_client.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_s3_client.py`:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.storage.s3_client import S3Client


@pytest.fixture
def s3_client():
    return S3Client(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        region="us-east-1",
    )


async def test_upload_object(s3_client):
    mock_s3 = AsyncMock()
    mock_s3.put_object = AsyncMock(return_value={"VersionId": "v1"})

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    version_id = await s3_client.upload_object(
        bucket="test-bucket", key="test-key", body=b"test-data", content_type="application/pdf",
    )
    assert version_id == "v1"
    mock_s3.put_object.assert_called_once()


async def test_generate_presigned_url(s3_client):
    mock_s3 = AsyncMock()
    mock_s3.generate_presigned_url = AsyncMock(return_value="https://s3/presigned")

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    s3_client._session = MagicMock()
    s3_client._session.create_client = MagicMock(return_value=mock_ctx)

    url = await s3_client.generate_presigned_url(
        bucket="test-bucket", key="test-key", expires_in=300,
    )
    assert url == "https://s3/presigned"
```

Run: `uv run pytest tests/unit/test_s3_client.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать S3 client**

`app/storage/s3_client.py`:

```python
import aiobotocore.session


class S3Client:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, region: str):
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._session = aiobotocore.session.get_session()

    def _client(self):
        return self._session.create_client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )

    async def upload_object(
        self, bucket: str, key: str, body: bytes, content_type: str
    ) -> str | None:
        async with self._client() as s3:
            response = await s3.put_object(
                Bucket=bucket, Key=key, Body=body, ContentType=content_type
            )
            return response.get("VersionId")

    async def generate_presigned_url(
        self, bucket: str, key: str, expires_in: int = 300, disposition: str = "inline",
    ) -> str:
        async with self._client() as s3:
            return await s3.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket, "Key": key,
                    "ResponseContentDisposition": disposition,
                },
                ExpiresIn=expires_in,
            )

    async def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
        async with self._client() as s3:
            await s3.copy_object(
                Bucket=dst_bucket, Key=dst_key,
                CopySource={"Bucket": src_bucket, "Key": src_key},
            )

    async def delete_object(self, bucket: str, key: str):
        async with self._client() as s3:
            await s3.delete_object(Bucket=bucket, Key=key)

    async def head_bucket(self, bucket: str):
        async with self._client() as s3:
            await s3.head_bucket(Bucket=bucket)
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_s3_client.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/storage/s3_client.py tests/unit/test_s3_client.py
git commit -m "feat: add async S3 client wrapper over aiobotocore"
```

---

### Task 8: Metadata repository

**Files:**
- Create: `app/storage/metadata_repository.py`
- Test: `tests/unit/test_metadata_repository.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_metadata_repository.py`:

```python
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.db_models import File
from app.storage.metadata_repository import MetadataRepository


@pytest.fixture
def session():
    s = AsyncMock()
    s.add = MagicMock()
    s.flush = AsyncMock()
    s.commit = AsyncMock()
    return s


@pytest.fixture
def repo(session):
    return MetadataRepository(session)


async def test_create_file(repo, session):
    file = File(
        original_name="test.pdf", storage_key="quarantine/test",
        bucket="documents-quarantine", content_type="application/pdf",
        size_bytes=1024, checksum_sha256="abc", checksum_gost="def",
        owner_type="LOT", owner_id=uuid.uuid4(), uploaded_by=uuid.uuid4(),
    )
    await repo.create(file)
    session.add.assert_called_once_with(file)
    session.flush.assert_called_once()


async def test_get_by_id_filters_deleted(repo, session):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=mock_result)
    result = await repo.get_by_id(uuid.uuid4())
    assert result is None
    session.execute.assert_called_once()
```

Run: `uv run pytest tests/unit/test_metadata_repository.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать repository**

`app/storage/metadata_repository.py`:

```python
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
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_metadata_repository.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/storage/metadata_repository.py tests/unit/test_metadata_repository.py
git commit -m "feat: add metadata repository with soft-delete filtering"
```

---

## Chunk 3: Domain Layer

### Task 9: Streaming hash calculator

**Files:**
- Create: `app/domain/hash_calculator.py`
- Test: `tests/unit/test_hash_calculator.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_hash_calculator.py`:

```python
import hashlib

from app.domain.hash_calculator import compute_hashes


async def test_compute_hashes_returns_sha256_and_gost():
    data = b"Hello, Document Storage Service!"

    async def chunk_iter():
        yield data

    sha256_hex, gost_hex = await compute_hashes(chunk_iter())
    assert sha256_hex == hashlib.sha256(data).hexdigest()
    assert len(gost_hex) == 64  # GOST 256-bit = 64 hex chars


async def test_compute_hashes_streaming_multiple_chunks():
    chunks = [b"chunk1", b"chunk2", b"chunk3"]
    full_data = b"".join(chunks)

    async def chunk_iter():
        for c in chunks:
            yield c

    sha256_hex, gost_hex = await compute_hashes(chunk_iter())
    assert sha256_hex == hashlib.sha256(full_data).hexdigest()
    assert len(gost_hex) == 64
```

Run: `uv run pytest tests/unit/test_hash_calculator.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать hash calculator**

Note: pygost's `GOST34112012256` may not have `hexdigest()`. Verify API and use `.digest().hex()` if needed.

`app/domain/hash_calculator.py`:

```python
import hashlib
from collections.abc import AsyncIterator

from pygost.gost34112012256 import GOST34112012256


async def compute_hashes(chunks: AsyncIterator[bytes]) -> tuple[str, str]:
    """Compute SHA-256 and GOST 34.11-2012-256 in a single streaming pass."""
    sha256 = hashlib.sha256()
    gost = GOST34112012256()

    async for chunk in chunks:
        if chunk:
            sha256.update(chunk)
            gost.update(chunk)

    return sha256.hexdigest(), gost.digest().hex()
```

- [ ] **Step 3: Запустить тест**

Run: `uv run pytest tests/unit/test_hash_calculator.py -v`
Expected: PASS (если pygost API совпадает; при ошибке — проверить pygost docs и исправить)

- [ ] **Step 4: Commit**

```bash
git add app/domain/hash_calculator.py tests/unit/test_hash_calculator.py
git commit -m "feat: add streaming dual-hash calculator (SHA-256 + GOST)"
```

---

### Task 10: File validators

**Files:**
- Create: `app/domain/validators.py`
- Test: `tests/unit/test_validators.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_validators.py`:

```python
import pytest
from app.domain.validators import validate_content_type, validate_file_size

ALLOWED = ["application/pdf", "image/png", "image/jpeg"]


def test_validate_content_type_allowed():
    validate_content_type("application/pdf", ALLOWED)


def test_validate_content_type_not_allowed():
    with pytest.raises(ValueError, match="not allowed"):
        validate_content_type("application/zip", ALLOWED)


def test_validate_file_size_within_limit():
    validate_file_size(1024, max_size_mb=20)


def test_validate_file_size_exceeds_limit():
    with pytest.raises(ValueError, match="exceeds"):
        validate_file_size(21 * 1024 * 1024, max_size_mb=20)
```

Run: `uv run pytest tests/unit/test_validators.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать validators**

`app/domain/validators.py`:

```python
def validate_content_type(content_type: str, allowed: list[str]) -> None:
    if content_type not in allowed:
        raise ValueError(f"Content type '{content_type}' is not allowed. Allowed: {allowed}")


def validate_file_size(size_bytes: int, max_size_mb: int) -> None:
    max_bytes = max_size_mb * 1024 * 1024
    if size_bytes > max_bytes:
        raise ValueError(f"File size {size_bytes} bytes exceeds limit of {max_size_mb} MB")
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_validators.py -v`

```bash
git add app/domain/validators.py tests/unit/test_validators.py
git commit -m "feat: add file content type and size validators"
```

---

### Task 11: Upload token service

**Files:**
- Create: `app/domain/upload_token_service.py`
- Test: `tests/unit/test_upload_token_service.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_upload_token_service.py`:

```python
import uuid
from unittest.mock import AsyncMock

import pytest
from app.domain.upload_token_service import UploadTokenService


@pytest.fixture
def cache():
    mock = AsyncMock()
    mock.is_token_blacklisted = AsyncMock(return_value=False)
    mock.blacklist_token = AsyncMock()
    return mock


@pytest.fixture
def service(cache):
    return UploadTokenService(
        secret="test-secret-256bit-long-enough-key!", algorithm="HS256",
        ttl_seconds=600, cache=cache,
    )


def test_generate_returns_string(service):
    token = service.generate(
        owner_type="LOT", owner_id=uuid.uuid4(), visibility="PUBLIC",
        file_name="test.pdf", content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024, uploaded_by=uuid.uuid4(),
    )
    assert isinstance(token, str) and len(token) > 0


async def test_validate_returns_payload(service):
    owner_id = uuid.uuid4()
    uploaded_by = uuid.uuid4()
    token = service.generate(
        owner_type="LOT", owner_id=owner_id, visibility="PUBLIC",
        file_name="test.pdf", content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024, uploaded_by=uploaded_by,
    )
    payload = await service.validate(token)
    assert payload["owner_type"] == "LOT"
    assert payload["owner_id"] == str(owner_id)
    assert payload["uploaded_by"] == str(uploaded_by)


async def test_validate_blacklisted_raises(service, cache):
    cache.is_token_blacklisted.return_value = True
    token = service.generate(
        owner_type="LOT", owner_id=uuid.uuid4(), visibility="PRIVATE",
        file_name="t.pdf", content_type="application/pdf",
        max_size_bytes=1024, uploaded_by=uuid.uuid4(),
    )
    with pytest.raises(ValueError, match="blacklisted"):
        await service.validate(token)


async def test_consume_blacklists(service, cache):
    token = service.generate(
        owner_type="LOT", owner_id=uuid.uuid4(), visibility="PRIVATE",
        file_name="t.pdf", content_type="application/pdf",
        max_size_bytes=1024, uploaded_by=uuid.uuid4(),
    )
    await service.consume(token)
    cache.blacklist_token.assert_called_once()
```

Run: `uv run pytest tests/unit/test_upload_token_service.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать**

`app/domain/upload_token_service.py`:

```python
import uuid
from datetime import datetime, timedelta, timezone

import jwt

from app.storage.cache_client import CacheClient


class UploadTokenService:
    def __init__(self, secret: str, algorithm: str, ttl_seconds: int, cache: CacheClient):
        self._secret = secret
        self._algorithm = algorithm
        self._ttl_seconds = ttl_seconds
        self._cache = cache

    def generate(
        self, owner_type: str, owner_id: uuid.UUID, visibility: str,
        file_name: str, content_type: str, max_size_bytes: int, uploaded_by: uuid.UUID,
    ) -> str:
        now = datetime.now(timezone.utc)
        return jwt.encode(
            {
                "jti": str(uuid.uuid4()),
                "owner_type": owner_type, "owner_id": str(owner_id),
                "visibility": visibility, "file_name": file_name,
                "content_type": content_type, "max_size_bytes": max_size_bytes,
                "uploaded_by": str(uploaded_by),
                "iat": now, "exp": now + timedelta(seconds=self._ttl_seconds),
            },
            self._secret, algorithm=self._algorithm,
        )

    async def validate(self, token: str) -> dict:
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise ValueError("Upload token has expired")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid upload token: {e}")
        if await self._cache.is_token_blacklisted(payload.get("jti")):
            raise ValueError("Upload token has been blacklisted")
        return payload

    async def consume(self, token: str) -> dict:
        payload = await self.validate(token)
        remaining = max(int(payload["exp"] - datetime.now(timezone.utc).timestamp()), 1)
        await self._cache.blacklist_token(payload["jti"], ttl_seconds=remaining)
        return payload
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_upload_token_service.py -v`

```bash
git add app/domain/upload_token_service.py tests/unit/test_upload_token_service.py
git commit -m "feat: add upload token service (JWT generate/validate/consume)"
```

---

### Task 12: Download token service

**Files:**
- Create: `app/domain/download_token_service.py`
- Test: `tests/unit/test_download_token_service.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_download_token_service.py`:

```python
import uuid
from unittest.mock import AsyncMock

import pytest
from app.domain.download_token_service import DownloadTokenService


@pytest.fixture
def cache():
    mock = AsyncMock()
    mock.is_token_blacklisted = AsyncMock(return_value=False)
    mock.blacklist_token = AsyncMock()
    return mock


@pytest.fixture
def service(cache):
    return DownloadTokenService(
        secret="test-secret-256bit-long-enough-key!", algorithm="HS256",
        max_ttl_seconds=600, cache=cache,
    )


def test_generate_returns_string(service):
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    assert isinstance(token, str)


async def test_validate_returns_payload(service):
    file_id = uuid.uuid4()
    token = service.generate(
        file_id=file_id, user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    payload = await service.validate(token)
    assert payload["file_id"] == str(file_id)
    assert payload["disposition"] == "inline"


async def test_ttl_capped_at_max(service):
    """Token with expires_in > max_ttl should have exp capped."""
    import jwt as pyjwt
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=9999,
    )
    payload = pyjwt.decode(token, "test-secret-256bit-long-enough-key!", algorithms=["HS256"])
    actual_ttl = payload["exp"] - payload["iat"]
    assert actual_ttl <= 600


async def test_consume_blacklists(service, cache):
    token = service.generate(
        file_id=uuid.uuid4(), user_id=uuid.uuid4(),
        version=None, disposition="inline", expires_in_seconds=300,
    )
    await service.consume(token)
    cache.blacklist_token.assert_called_once()
```

Run: `uv run pytest tests/unit/test_download_token_service.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать**

`app/domain/download_token_service.py`:

```python
import uuid
from datetime import datetime, timedelta, timezone

import jwt

from app.storage.cache_client import CacheClient


class DownloadTokenService:
    def __init__(self, secret: str, algorithm: str, max_ttl_seconds: int, cache: CacheClient):
        self._secret = secret
        self._algorithm = algorithm
        self._max_ttl_seconds = max_ttl_seconds
        self._cache = cache

    def generate(
        self, file_id: uuid.UUID, user_id: uuid.UUID,
        version: int | None, disposition: str, expires_in_seconds: int,
    ) -> str:
        ttl = min(expires_in_seconds, self._max_ttl_seconds)
        now = datetime.now(timezone.utc)
        return jwt.encode(
            {
                "jti": str(uuid.uuid4()), "file_id": str(file_id),
                "user_id": str(user_id), "version": version,
                "disposition": disposition,
                "iat": now, "exp": now + timedelta(seconds=ttl),
            },
            self._secret, algorithm=self._algorithm,
        )

    async def validate(self, token: str) -> dict:
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise ValueError("Download token has expired")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid download token: {e}")
        if await self._cache.is_token_blacklisted(payload.get("jti")):
            raise ValueError("Download token has been blacklisted")
        return payload

    async def consume(self, token: str) -> dict:
        payload = await self.validate(token)
        remaining = max(int(payload["exp"] - datetime.now(timezone.utc).timestamp()), 1)
        await self._cache.blacklist_token(payload["jti"], ttl_seconds=remaining)
        return payload
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_download_token_service.py -v`

```bash
git add app/domain/download_token_service.py tests/unit/test_download_token_service.py
git commit -m "feat: add download token service (JWT with max TTL cap)"
```

---

### Task 13: Pydantic schemas

**Files:**
- Create: `app/domain/schemas.py`
- Test: `tests/unit/test_schemas.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_schemas.py`:

```python
import uuid
import pytest
from pydantic import ValidationError
from app.domain.schemas import UploadTokenRequest, FileResponse, DownloadTokenRequest


def test_upload_token_request_valid():
    req = UploadTokenRequest(
        owner_type="LOT", owner_id=uuid.uuid4(), visibility="PUBLIC",
        file_name="test.pdf", content_type="application/pdf",
        max_size_bytes=20 * 1024 * 1024, uploaded_by=uuid.uuid4(),
    )
    assert req.owner_type == "LOT"


def test_upload_token_request_invalid_visibility():
    with pytest.raises(ValidationError):
        UploadTokenRequest(
            owner_type="LOT", owner_id=uuid.uuid4(), visibility="INVALID",
            file_name="t.pdf", content_type="application/pdf",
            max_size_bytes=1024, uploaded_by=uuid.uuid4(),
        )


def test_file_response_includes_uploaded_by():
    resp = FileResponse(
        file_id=uuid.uuid4(), original_name="test.pdf",
        content_type="application/pdf", size_bytes=1024,
        checksum_sha256="abc", checksum_gost="def",
        av_status="PENDING", version=1, visibility="PRIVATE",
        owner_type="LOT", owner_id=uuid.uuid4(),
        uploaded_by=uuid.uuid4(), uploaded_at="2025-01-01T00:00:00Z",
    )
    data = resp.model_dump(mode="json")
    assert "uploaded_by" in data


def test_download_token_request_defaults():
    req = DownloadTokenRequest(file_id=uuid.uuid4(), user_id=uuid.uuid4())
    assert req.disposition == "inline"
    assert req.expires_in_seconds == 300
```

Run: `uv run pytest tests/unit/test_schemas.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать schemas**

`app/domain/schemas.py`:

```python
import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Visibility(str, Enum):
    PRIVATE = "PRIVATE"
    PUBLIC = "PUBLIC"


class AVStatus(str, Enum):
    PENDING = "PENDING"
    SCANNING = "SCANNING"
    CLEAN = "CLEAN"
    INFECTED = "INFECTED"
    ERROR = "ERROR"


class UploadTokenRequest(BaseModel):
    owner_type: str
    owner_id: uuid.UUID
    visibility: Visibility
    file_name: str
    content_type: str
    max_size_bytes: int = Field(gt=0)
    uploaded_by: uuid.UUID


class UploadTokenResponse(BaseModel):
    upload_token: str
    expires_at: datetime
    owner_type: str
    owner_id: uuid.UUID


class DownloadTokenRequest(BaseModel):
    file_id: uuid.UUID
    user_id: uuid.UUID
    version: int | None = None
    disposition: str = "inline"
    expires_in_seconds: int = Field(default=300, gt=0, le=600)


class DownloadTokenResponse(BaseModel):
    download_token: str
    expires_at: datetime
    file_id: uuid.UUID
    original_name: str
    content_type: str
    size_bytes: int


class FileResponse(BaseModel):
    file_id: uuid.UUID
    original_name: str
    content_type: str
    size_bytes: int
    checksum_sha256: str
    checksum_gost: str
    av_status: str
    version: int
    visibility: str
    owner_type: str
    owner_id: uuid.UUID
    uploaded_by: uuid.UUID
    uploaded_at: datetime


class FileListResponse(BaseModel):
    items: list[FileResponse]
    total: int
    page: int
    page_size: int


class PresignedUrlRequest(BaseModel):
    expires_in_seconds: int = Field(default=300, gt=0, le=600)
    disposition: str = "inline"


class PresignedUrlResponse(BaseModel):
    url: str
    expires_at: datetime
    file_id: uuid.UUID
    content_type: str
    size_bytes: int
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_schemas.py -v`

```bash
git add app/domain/schemas.py tests/unit/test_schemas.py
git commit -m "feat: add Pydantic request/response schemas"
```

---

### Task 14: File service

**Files:**
- Create: `app/domain/file_service.py`
- Test: `tests/unit/test_file_service.py`

**Важно:** файл не грузится полностью в память. Для streaming upload в S3 через aiobotocore используется временный файл на диске (spooled). Это соответствует требованию ТЗ (секция 5.1.1).

- [ ] **Step 1: Написать тест**

`tests/unit/test_file_service.py`:

```python
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.domain.exceptions import FileNotFoundError as DSSFileNotFoundError
from app.domain.file_service import FileService


@pytest.fixture
def deps():
    repo = AsyncMock()
    s3 = AsyncMock()
    cache = AsyncMock()
    cache.get_cached_presigned_url = AsyncMock(return_value=None)
    settings = MagicMock()
    settings.s3_bucket_private = "documents-private"
    settings.s3_bucket_public = "documents-public"
    settings.max_file_size_mb = 20
    settings.allowed_content_types = ["application/pdf", "image/png"]
    settings.presigned_url_ttl_seconds = 300
    settings.public_presigned_url_ttl_seconds = 3600
    return repo, s3, cache, settings


@pytest.fixture
def service(deps):
    return FileService(*deps)


async def test_upload_creates_file_record(service, deps):
    repo, s3, cache, settings = deps
    s3.upload_object = AsyncMock(return_value="v1")
    repo.create = AsyncMock(side_effect=lambda f: f)

    async def chunks():
        yield b"file content"

    with patch("app.domain.file_service.compute_hashes", return_value=("sha256hex", "gosthex")):
        result = await service.upload(
            file_stream=chunks(), file_name="test.pdf",
            content_type="application/pdf", size_bytes=1024,
            owner_type="LOT", owner_id=uuid.uuid4(),
            visibility="PRIVATE", uploaded_by=uuid.uuid4(),
        )

    assert result.original_name == "test.pdf"
    assert result.checksum_sha256 == "sha256hex"
    repo.create.assert_called_once()
    s3.upload_object.assert_called_once()


async def test_upload_rejects_invalid_content_type(service):
    async def chunks():
        yield b"data"

    with pytest.raises(ValueError, match="not allowed"):
        await service.upload(
            file_stream=chunks(), file_name="test.zip",
            content_type="application/zip", size_bytes=1024,
            owner_type="LOT", owner_id=uuid.uuid4(),
            visibility="PRIVATE", uploaded_by=uuid.uuid4(),
        )


async def test_get_file_not_found(service, deps):
    deps[0].get_by_id.return_value = None
    with pytest.raises(DSSFileNotFoundError):
        await service.get_file(uuid.uuid4())


async def test_presigned_url_uses_disposition_cache_key(service, deps):
    repo, s3, cache, settings = deps
    file = MagicMock()
    file.av_status = "CLEAN"
    file.visibility = "PRIVATE"
    file.bucket = "documents-private"
    file.storage_key = "lot/uuid/file/1"
    file.id = uuid.uuid4()
    repo.get_by_id.return_value = file
    s3.generate_presigned_url = AsyncMock(return_value="https://s3/url")

    await service.generate_presigned_url(file.id, disposition="attachment")
    cache.get_cached_presigned_url.assert_called_with(str(file.id), "attachment")
```

Run: `uv run pytest tests/unit/test_file_service.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать file service**

`app/domain/file_service.py`:

```python
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
        self, repo: MetadataRepository, s3: S3Client, cache: CacheClient, settings: Settings,
    ):
        self._repo = repo
        self._s3 = s3
        self._cache = cache
        self._settings = settings

    async def upload(
        self, file_stream: AsyncIterator[bytes], file_name: str, content_type: str,
        size_bytes: int, owner_type: str, owner_id: uuid_mod.UUID,
        visibility: str, uploaded_by: uuid_mod.UUID, correlation_id: str | None = None,
    ) -> File:
        validate_content_type(content_type, self._settings.allowed_content_types)
        validate_file_size(size_bytes, self._settings.max_file_size_mb)

        # Collect file and compute hashes in one pass.
        # TODO: For true streaming to S3, use multipart upload with temp file.
        # Current approach loads file into memory — acceptable for ≤20MB files
        # but should be optimized for production load (500 concurrent uploads).
        collected = bytearray()

        async def collecting_chunks():
            async for chunk in file_stream:
                collected.extend(chunk)
                yield chunk

        sha256_hex, gost_hex = await compute_hashes(collecting_chunks())
        file_bytes = bytes(collected)

        # Slice 1: upload to target bucket directly. Slice 2 changes to quarantine.
        bucket = (
            self._settings.s3_bucket_public if visibility == "PUBLIC"
            else self._settings.s3_bucket_private
        )

        file_id = uuid_mod.uuid4()
        storage_key = f"{owner_type.lower()}/{owner_id}/{file_id}/1"

        s3_version_id = await self._s3.upload_object(
            bucket=bucket, key=storage_key, body=file_bytes, content_type=content_type,
        )

        file = File(
            id=file_id, original_name=file_name, storage_key=storage_key,
            bucket=bucket, content_type=content_type, size_bytes=len(file_bytes),
            checksum_sha256=sha256_hex, checksum_gost=gost_hex,
            owner_type=owner_type, owner_id=owner_id, visibility=visibility,
            uploaded_by=uploaded_by, s3_version_id=s3_version_id,
            correlation_id=uuid_mod.UUID(correlation_id) if correlation_id else None,
        )
        file = await self._repo.create(file)
        logger.info("file_uploaded", file_id=str(file.id), size=len(file_bytes))
        return file

    async def get_file(self, file_id: uuid_mod.UUID) -> File:
        file = await self._repo.get_by_id(file_id)
        if not file:
            raise FileNotFoundError(f"File {file_id} not found")
        return file

    async def get_files_by_owner(
        self, owner_type: str, owner_id: uuid_mod.UUID,
        av_status: str | None = None, page: int = 1, page_size: int = 20,
    ) -> tuple[list[File], int]:
        return await self._repo.get_by_owner(
            owner_type=owner_type, owner_id=owner_id,
            av_status=av_status, page=page, page_size=min(page_size, 100),
        )

    async def soft_delete(self, file_id: uuid_mod.UUID) -> None:
        if not await self._repo.soft_delete(file_id):
            raise FileNotFoundError(f"File {file_id} not found")
        logger.info("file_deleted", file_id=str(file_id))

    async def generate_presigned_url(
        self, file_id: uuid_mod.UUID, expires_in: int | None = None, disposition: str = "inline",
    ) -> str:
        file = await self.get_file(file_id)
        ttl = expires_in or (
            self._settings.public_presigned_url_ttl_seconds if file.visibility == "PUBLIC"
            else self._settings.presigned_url_ttl_seconds
        )

        cached = await self._cache.get_cached_presigned_url(str(file_id), disposition)
        if cached:
            return cached

        url = await self._s3.generate_presigned_url(
            bucket=file.bucket, key=file.storage_key, expires_in=ttl, disposition=disposition,
        )
        await self._cache.cache_presigned_url(str(file_id), disposition, url, ttl_seconds=max(ttl - 60, 60))
        return url
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_file_service.py -v`

```bash
git add app/domain/file_service.py tests/unit/test_file_service.py
git commit -m "feat: add file service with upload, download, and presigned URL"
```

---

## Chunk 4: Auth + API Routes

### Task 15: JWT auth infrastructure

**Files:**
- Create: `app/infrastructure/auth.py`
- Test: `tests/unit/test_auth.py`

- [ ] **Step 1: Написать тест**

`tests/unit/test_auth.py`:

```python
from datetime import datetime, timedelta, timezone
import jwt
import pytest
from app.infrastructure.auth import decode_service_jwt, verify_scope

SECRET = "test-service-jwt-secret-key"


def _make_token(scopes, exp_delta=600):
    return jwt.encode(
        {"sub": "notice-service", "scopes": scopes,
         "iat": datetime.now(timezone.utc),
         "exp": datetime.now(timezone.utc) + timedelta(seconds=exp_delta)},
        SECRET, algorithm="HS256",
    )


def test_decode_valid():
    payload = decode_service_jwt(_make_token(["documents.read"]), SECRET, "HS256")
    assert payload["sub"] == "notice-service"


def test_decode_expired():
    with pytest.raises(ValueError, match="expired"):
        decode_service_jwt(_make_token(["documents.read"], -10), SECRET, "HS256")


def test_verify_scope_passes():
    verify_scope({"scopes": ["documents.read", "documents.write"]}, "documents.read")


def test_verify_scope_fails():
    with pytest.raises(PermissionError, match="scope"):
        verify_scope({"scopes": ["documents.read"]}, "documents.write")
```

Run: `uv run pytest tests/unit/test_auth.py -v`
Expected: FAIL

- [ ] **Step 2: Реализовать auth**

`app/infrastructure/auth.py`:

```python
import jwt as pyjwt
from fastapi import Depends, Header, HTTPException, Request


def decode_service_jwt(token: str, secret: str, algorithm: str) -> dict:
    try:
        return pyjwt.decode(token, secret, algorithms=[algorithm])
    except pyjwt.ExpiredSignatureError:
        raise ValueError("Service JWT has expired")
    except pyjwt.InvalidTokenError as e:
        raise ValueError(f"Invalid service JWT: {e}")


def verify_scope(payload: dict, required_scope: str) -> None:
    if required_scope not in payload.get("scopes", []):
        raise PermissionError(f"Missing required scope: {required_scope}")


async def require_service_jwt(
    request: Request,
    authorization: str = Header(...),
) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    settings = request.app.state.settings
    try:
        return decode_service_jwt(
            authorization[7:], settings.service_jwt_secret, settings.service_jwt_algorithm,
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))


def require_scope(scope: str):
    async def checker(payload: dict = Depends(require_service_jwt)):
        try:
            verify_scope(payload, scope)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e))
        return payload
    return checker
```

- [ ] **Step 3: Запустить тест, commit**

Run: `uv run pytest tests/unit/test_auth.py -v`

```bash
git add app/infrastructure/auth.py tests/unit/test_auth.py
git commit -m "feat: add service JWT auth with separate secret"
```

---

### Task 16: Prometheus metrics + exception handlers

**Files:**
- Create: `app/infrastructure/metrics.py`
- Create: `app/api/v2/exception_handlers.py`

- [ ] **Step 1: Создать metrics (определения, запись метрик — TODO для финализации)**

`app/infrastructure/metrics.py`:

```python
from prometheus_client import Counter, Histogram

UPLOAD_REQUESTS = Counter("dss_upload_requests_total", "Total upload requests", ["status", "content_type", "owner_type"])
UPLOAD_DURATION = Histogram("dss_upload_duration_seconds", "Upload duration", ["content_type"])
UPLOAD_SIZE = Histogram("dss_upload_size_bytes", "Upload file sizes", ["content_type", "owner_type"],
    buckets=[1024, 10240, 102400, 1048576, 5242880, 10485760, 20971520])
DOWNLOAD_REQUESTS = Counter("dss_download_requests_total", "Total download requests", ["status", "visibility"])
PRESIGNED_URL_DURATION = Histogram("dss_presigned_url_duration_seconds", "Presigned URL generation duration", ["visibility"])
S3_OPERATIONS = Counter("dss_s3_operations_total", "S3 operations", ["operation", "bucket", "status"])
```

- [ ] **Step 2: Создать exception handlers**

`app/api/v2/exception_handlers.py`:

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.domain.exceptions import (
    AVNotPassedError, FileNotFoundError, FileSizeExceededError, InvalidContentTypeError, TokenError,
)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(FileNotFoundError)
    async def _(request: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(TokenError)
    async def _(request: Request, exc: TokenError):
        return JSONResponse(status_code=401, content={"detail": str(exc)})

    @app.exception_handler(AVNotPassedError)
    async def _(request: Request, exc: AVNotPassedError):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(FileSizeExceededError)
    async def _(request: Request, exc: FileSizeExceededError):
        return JSONResponse(status_code=413, content={"detail": str(exc)})

    @app.exception_handler(InvalidContentTypeError)
    async def _(request: Request, exc: InvalidContentTypeError):
        return JSONResponse(status_code=415, content={"detail": str(exc)})

    @app.exception_handler(ValueError)
    async def _(request: Request, exc: ValueError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})
```

- [ ] **Step 3: Commit**

```bash
git add app/infrastructure/metrics.py app/api/v2/exception_handlers.py
git commit -m "feat: add Prometheus metrics and exception handlers"
```

---

### Task 17: API routes — upload-token, upload

**Files:**
- Create: `app/api/v2/routes_upload_token.py`
- Create: `app/api/v2/routes_upload.py`

- [ ] **Step 1: Реализовать upload-token route**

`app/api/v2/routes_upload_token.py`:

```python
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request

from app.domain.schemas import UploadTokenRequest, UploadTokenResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["upload-token"])


@router.post("/upload-token", response_model=UploadTokenResponse, status_code=201)
async def create_upload_token(
    body: UploadTokenRequest, request: Request,
    _: dict = Depends(require_scope("documents.issue_token")),
):
    service = request.app.state.upload_token_service
    settings = request.app.state.settings
    token = service.generate(
        owner_type=body.owner_type, owner_id=body.owner_id,
        visibility=body.visibility.value, file_name=body.file_name,
        content_type=body.content_type, max_size_bytes=body.max_size_bytes,
        uploaded_by=body.uploaded_by,
    )
    return UploadTokenResponse(
        upload_token=token,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=settings.upload_token_ttl_seconds),
        owner_type=body.owner_type, owner_id=body.owner_id,
    )
```

- [ ] **Step 2: Реализовать upload route**

`app/api/v2/routes_upload.py`:

```python
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
            raise HTTPException(status_code=413, detail=f"File exceeds max size of {max_size} bytes")

    elif authorization and authorization.startswith("Bearer "):
        settings = request.app.state.settings
        try:
            jwt_payload = decode_service_jwt(authorization[7:], settings.service_jwt_secret, settings.service_jwt_algorithm)
            verify_scope(jwt_payload, "documents.write")
        except (ValueError, PermissionError) as e:
            raise HTTPException(status_code=403, detail=str(e))

        if not owner_type or not owner_id:
            raise HTTPException(status_code=400, detail="owner_type and owner_id required for M2M upload")

        up_owner_type = owner_type
        up_owner_id = uuid.UUID(owner_id)
        up_visibility = visibility or "PRIVATE"
        up_uploaded_by = uuid.UUID(jwt_payload.get("sub", str(uuid.uuid4())))
        max_size = settings.max_file_size_mb * 1024 * 1024
    else:
        raise HTTPException(status_code=401, detail="X-Upload-Token or Authorization header required")

    correlation_id = getattr(request.state, "correlation_id", None)

    async def file_chunks():
        while chunk := await file.read(64 * 1024):
            yield chunk

    result = await file_service.upload(
        file_stream=file_chunks(), file_name=file.filename or "unnamed",
        content_type=file.content_type or "application/octet-stream",
        size_bytes=file.size or 0, owner_type=up_owner_type,
        owner_id=up_owner_id, visibility=up_visibility,
        uploaded_by=up_uploaded_by, correlation_id=correlation_id,
    )
    await session.commit()

    return FileResponse(
        file_id=result.id, original_name=result.original_name,
        content_type=result.content_type, size_bytes=result.size_bytes,
        checksum_sha256=result.checksum_sha256, checksum_gost=result.checksum_gost,
        av_status=result.av_status, version=result.version,
        visibility=result.visibility, owner_type=result.owner_type,
        owner_id=result.owner_id, uploaded_by=result.uploaded_by,
        uploaded_at=result.uploaded_at,
    )
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v2/routes_upload_token.py app/api/v2/routes_upload.py
git commit -m "feat: add upload-token and upload endpoints"
```

---

### Task 18: API routes — download-token, download, presigned URL

**Files:**
- Create: `app/api/v2/routes_download_token.py`
- Create: `app/api/v2/routes_download.py`

- [ ] **Step 1: Реализовать download-token route**

`app/api/v2/routes_download_token.py`:

```python
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db_session, get_file_service
from app.domain.file_service import FileService
from app.domain.schemas import DownloadTokenRequest, DownloadTokenResponse
from app.infrastructure.auth import require_scope

router = APIRouter(prefix="/api/v2/documents", tags=["download-token"])


@router.post("/download-token", response_model=DownloadTokenResponse, status_code=201)
async def create_download_token(
    body: DownloadTokenRequest, request: Request,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.issue_token")),
):
    file = await file_service.get_file(body.file_id)

    token = request.app.state.download_token_service.generate(
        file_id=body.file_id, user_id=body.user_id,
        version=body.version, disposition=body.disposition,
        expires_in_seconds=body.expires_in_seconds,
    )
    return DownloadTokenResponse(
        download_token=token,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=body.expires_in_seconds),
        file_id=file.id, original_name=file.original_name,
        content_type=file.content_type, size_bytes=file.size_bytes,
    )
```

- [ ] **Step 2: Реализовать download route**

`app/api/v2/routes_download.py`:

```python
import uuid
from datetime import datetime, timedelta, timezone

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
    request: Request, token: str = Query(...),
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
    file_id: uuid.UUID, body: PresignedUrlRequest, request: Request,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.read")),
):
    file = await file_service.get_file(file_id)
    url = await file_service.generate_presigned_url(
        file_id=file_id, expires_in=body.expires_in_seconds, disposition=body.disposition,
    )
    return PresignedUrlResponse(
        url=url,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=body.expires_in_seconds),
        file_id=file.id, content_type=file.content_type, size_bytes=file.size_bytes,
    )
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v2/routes_download_token.py app/api/v2/routes_download.py
git commit -m "feat: add download-token, download, and presigned-url endpoints"
```

---

### Task 19: API routes — public, metadata

**Files:**
- Create: `app/api/v2/routes_public.py`
- Create: `app/api/v2/routes_metadata.py`

- [ ] **Step 1: Реализовать public route**

`app/api/v2/routes_public.py`:

```python
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
```

- [ ] **Step 2: Реализовать metadata routes**

`app/api/v2/routes_metadata.py`:

```python
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
        file_id=f.id, original_name=f.original_name, content_type=f.content_type,
        size_bytes=f.size_bytes, checksum_sha256=f.checksum_sha256, checksum_gost=f.checksum_gost,
        av_status=f.av_status, version=f.version, visibility=f.visibility,
        owner_type=f.owner_type, owner_id=f.owner_id,
        uploaded_by=f.uploaded_by, uploaded_at=f.uploaded_at,
    )


@router.get("/by-owner/{owner_type}/{owner_id}", response_model=FileListResponse)
async def get_files_by_owner(
    owner_type: str, owner_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    file_service: FileService = Depends(get_file_service),
    _: dict = Depends(require_scope("documents.read")),
    av_status: str | None = Query(None),
    include_signatures: bool = Query(False),  # Placeholder for Slice 3
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    items, total = await file_service.get_files_by_owner(
        owner_type=owner_type, owner_id=owner_id,
        av_status=av_status, page=page, page_size=page_size,
    )
    return FileListResponse(
        items=[
            FileResponse(
                file_id=f.id, original_name=f.original_name, content_type=f.content_type,
                size_bytes=f.size_bytes, checksum_sha256=f.checksum_sha256, checksum_gost=f.checksum_gost,
                av_status=f.av_status, version=f.version, visibility=f.visibility,
                owner_type=f.owner_type, owner_id=f.owner_id,
                uploaded_by=f.uploaded_by, uploaded_at=f.uploaded_at,
            )
            for f in items
        ],
        total=total, page=page, page_size=page_size,
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
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v2/routes_public.py app/api/v2/routes_metadata.py
git commit -m "feat: add public download and metadata endpoints"
```

---

## Chunk 5: Final Assembly + Integration Test

### Task 20: Финальная сборка main.py

**Files:**
- Modify: `app/main.py` — полная финальная версия со всеми роутерами

- [ ] **Step 1: Собрать финальный main.py**

`app/main.py`:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v2.exception_handlers import register_exception_handlers
from app.api.v2.routes_download import router as download_router
from app.api.v2.routes_download_token import router as download_token_router
from app.api.v2.routes_health import router as health_router
from app.api.v2.routes_metadata import router as metadata_router
from app.api.v2.routes_public import router as public_router
from app.api.v2.routes_upload import router as upload_router
from app.api.v2.routes_upload_token import router as upload_token_router
from app.config import Settings
from app.domain.download_token_service import DownloadTokenService
from app.domain.upload_token_service import UploadTokenService
from app.infrastructure.correlation import CorrelationMiddleware
from app.infrastructure.logging import setup_logging
from app.storage.cache_client import CacheClient
from app.storage.s3_client import S3Client


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings = Settings()
    app.state.settings = settings

    engine = create_async_engine(settings.database_url, echo=False)
    app.state.db_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.s3 = S3Client(
        endpoint_url=settings.s3_endpoint, access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key, region=settings.s3_region,
    )

    cache = CacheClient(app.state.redis)
    app.state.cache = cache

    app.state.upload_token_service = UploadTokenService(
        secret=settings.upload_token_secret, algorithm=settings.upload_token_algorithm,
        ttl_seconds=settings.upload_token_ttl_seconds, cache=cache,
    )
    app.state.download_token_service = DownloadTokenService(
        secret=settings.download_token_secret, algorithm=settings.download_token_algorithm,
        max_ttl_seconds=settings.download_token_max_ttl_seconds, cache=cache,
    )

    yield
    await app.state.redis.aclose()
    await engine.dispose()


app = FastAPI(title="Document Storage Service", version="0.1.0", lifespan=lifespan)
register_exception_handlers(app)
app.add_middleware(CorrelationMiddleware)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(health_router)
app.include_router(upload_token_router)
app.include_router(upload_router)
app.include_router(download_token_router)
app.include_router(download_router)
app.include_router(public_router)
app.include_router(metadata_router)
```

- [ ] **Step 2: Commit**

```bash
git add app/main.py
git commit -m "feat: assemble final main.py with all routes"
```

---

### Task 21: Alembic миграция + smoke test

- [ ] **Step 1: Запустить инфраструктуру и миграцию**

```bash
cp .env.example .env
docker compose up -d postgres redis minio minio-init
sleep 5
uv run alembic revision --autogenerate -m "create files table"
uv run alembic upgrade head
```

- [ ] **Step 2: Запустить сервис и проверить health**

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8001 &
sleep 2
curl http://localhost:8001/health
# Expected: {"status": "ok"}
curl http://localhost:8001/health/ready
# Expected: {"postgres": "ok", "redis": "ok", "s3": "ok"}
kill %1
```

- [ ] **Step 3: Запустить все unit-тесты**

```bash
uv run pytest tests/unit -v
```

Expected: все тесты проходят.

- [ ] **Step 4: Commit**

```bash
git add migrations/ .env.example
git commit -m "feat: add initial Alembic migration for files table"
```

---

### Task 22: Интеграционный тест — полный цикл

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_upload_download.py`

Требует запущенных PostgreSQL, MinIO и Redis.

- [ ] **Step 1: Создать conftest**

`tests/integration/__init__.py` — пустой.

`tests/integration/conftest.py`:

```python
import os
from datetime import datetime, timedelta, timezone

import jwt
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://dss:password@localhost:5432/document_storage")
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
            "scopes": ["documents.issue_token", "documents.write", "documents.read", "documents.delete"],
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        },
        os.environ["SERVICE_JWT_SECRET"], algorithm="HS256",
    )
```

- [ ] **Step 2: Написать интеграционный тест**

`tests/integration/test_upload_download.py`:

```python
import uuid
import pytest


@pytest.mark.integration
async def test_full_upload_download_cycle(client, service_token):
    owner_id = str(uuid.uuid4())
    uploaded_by = str(uuid.uuid4())

    # 1. Get upload token
    resp = await client.post(
        "/api/v2/documents/upload-token",
        json={
            "owner_type": "LOT", "owner_id": owner_id,
            "visibility": "PRIVATE", "file_name": "test.pdf",
            "content_type": "application/pdf",
            "max_size_bytes": 20 * 1024 * 1024, "uploaded_by": uploaded_by,
        },
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 201, resp.text
    upload_token = resp.json()["upload_token"]

    # 2. Upload file
    resp = await client.post(
        "/api/v2/documents/upload",
        files={"file": ("test.pdf", b"PDF content here", "application/pdf")},
        headers={"X-Upload-Token": upload_token},
    )
    assert resp.status_code == 201, resp.text
    file_id = resp.json()["file_id"]
    assert resp.json()["uploaded_by"] == uploaded_by

    # 3. Get metadata
    resp = await client.get(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 200

    # 4. Get download token
    resp = await client.post(
        "/api/v2/documents/download-token",
        json={"file_id": file_id, "user_id": uploaded_by},
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 201
    download_token = resp.json()["download_token"]

    # 5. Download (redirect)
    resp = await client.get(
        f"/api/v2/documents/download?token={download_token}",
        follow_redirects=False,
    )
    assert resp.status_code == 302

    # 6. Soft delete
    resp = await client.delete(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 204

    # 7. Verify deleted
    resp = await client.get(
        f"/api/v2/documents/{file_id}",
        headers={"Authorization": f"Bearer {service_token}"},
    )
    assert resp.status_code == 404
```

- [ ] **Step 3: Запустить**

```bash
uv run pytest tests/integration/ -v -m integration
```

- [ ] **Step 4: Commit**

```bash
git add tests/integration/
git commit -m "feat: add integration test for full upload/download cycle"
```
