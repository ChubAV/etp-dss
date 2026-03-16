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
