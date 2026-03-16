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
    allowed_content_types: list[str] | str = [
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
